/*
 *  Copyright 2004 The WebRTC Project Authors. All rights reserved.
 *
 *  Use of this source code is governed by a BSD-style license
 *  that can be found in the LICENSE file in the root of the source
 *  tree. An additional intellectual property rights grant can be found
 *  in the file PATENTS.  All contributing project authors may
 *  be found in the AUTHORS file in the root of the source tree.
 */

#include "p2p/base/p2p_transport_channel.h"

#include <iterator>
#include <memory>
#include <set>
#include <utility>

#include "absl/algorithm/container.h"
#include "absl/strings/match.h"
#include "api/candidate.h"
#include "logging/rtc_event_log/ice_logger.h"
#include "p2p/base/basic_ice_controller.h"
#include "p2p/base/candidate_pair_interface.h"
#include "p2p/base/connection.h"
#include "p2p/base/port.h"
#include "rtc_base/checks.h"
#include "rtc_base/crc32.h"
#include "rtc_base/experiments/struct_parameters_parser.h"
#include "rtc_base/logging.h"
#include "rtc_base/net_helper.h"
#include "rtc_base/net_helpers.h"
#include "rtc_base/string_encode.h"
#include "rtc_base/time_utils.h"
#include "system_wrappers/include/field_trial.h"
#include "system_wrappers/include/metrics.h"

namespace {

cricket::PortInterface::CandidateOrigin GetOrigin(
    cricket::PortInterface* port,
    cricket::PortInterface* origin_port) {
  if (!origin_port)
    return cricket::PortInterface::ORIGIN_MESSAGE;
  else if (port == origin_port)
    return cricket::PortInterface::ORIGIN_THIS_PORT;
  else
    return cricket::PortInterface::ORIGIN_OTHER_PORT;
}

uint32_t GetWeakPingIntervalInFieldTrial() {
  uint32_t weak_ping_interval = ::strtoul(
      webrtc::field_trial::FindFullName("WebRTC-StunInterPacketDelay").c_str(),
      nullptr, 10);
  if (weak_ping_interval) {
    return static_cast<int>(weak_ping_interval);
  }
  return cricket::WEAK_PING_INTERVAL;
}

}  // unnamed namespace

namespace cricket {

using webrtc::RTCError;
using webrtc::RTCErrorType;

bool IceCredentialsChanged(const std::string& old_ufrag,
                           const std::string& old_pwd,
                           const std::string& new_ufrag,
                           const std::string& new_pwd) {
  // The standard (RFC 5245 Section 9.1.1.1) says that ICE restarts MUST change
  // both the ufrag and password. However, section 9.2.1.1 says changing the
  // ufrag OR password indicates an ICE restart. So, to keep compatibility with
  // endpoints that only change one, we'll treat this as an ICE restart.
  return (old_ufrag != new_ufrag) || (old_pwd != new_pwd);
}

P2PTransportChannel::P2PTransportChannel(const std::string& transport_name,
                                         int component,
                                         PortAllocator* allocator)
    : P2PTransportChannel(transport_name,
                          component,
                          allocator,
                          nullptr,
                          nullptr) {}

P2PTransportChannel::P2PTransportChannel(
    const std::string& transport_name,
    int component,
    PortAllocator* allocator,
    webrtc::AsyncResolverFactory* async_resolver_factory,
    webrtc::RtcEventLog* event_log,
    IceControllerFactoryInterface* ice_controller_factory)
    : transport_name_(transport_name),
      component_(component),
      allocator_(allocator),
      async_resolver_factory_(async_resolver_factory),
      network_thread_(rtc::Thread::Current()),
      incoming_only_(false),
      error_(0),
      sort_dirty_(false),
      remote_ice_mode_(ICEMODE_FULL),
      ice_role_(ICEROLE_UNKNOWN),
      tiebreaker_(0),
      gathering_state_(kIceGatheringNew),
      config_(RECEIVING_TIMEOUT,
              BACKUP_CONNECTION_PING_INTERVAL,
              GATHER_ONCE /* continual_gathering_policy */,
              false /* prioritize_most_likely_candidate_pairs */,
              STRONG_AND_STABLE_WRITABLE_CONNECTION_PING_INTERVAL,
              true /* presume_writable_when_fully_relayed */,
              REGATHER_ON_FAILED_NETWORKS_INTERVAL,
              RECEIVING_SWITCHING_DELAY) {
  RTC_DCHECK(allocator_ != nullptr);
  weak_ping_interval_ = GetWeakPingIntervalInFieldTrial();
  // Validate IceConfig even for mostly built-in constant default values in case
  // we change them.
  RTC_DCHECK(ValidateIceConfig(config_).ok());
  webrtc::BasicRegatheringController::Config regathering_config(
      config_.regather_all_networks_interval_range,
      config_.regather_on_failed_networks_interval_or_default());
  regathering_controller_ =
      std::make_unique<webrtc::BasicRegatheringController>(
          regathering_config, this, network_thread_);
  // We populate the change in the candidate filter to the session taken by
  // the transport.
  allocator_->SignalCandidateFilterChanged.connect(
      this, &P2PTransportChannel::OnCandidateFilterChanged);
  ice_event_log_.set_event_log(event_log);

  IceControllerFactoryArgs args{
      [this] { return GetState(); },
      [this] { return GetIceRole(); },
      [this](const Connection* connection) {
        return IsPortPruned(connection->port()) ||
               IsRemoteCandidatePruned(connection->remote_candidate());
      },
      &field_trials_,
  };
  if (ice_controller_factory != nullptr) {
    ice_controller_ = ice_controller_factory->Create(args);
  } else {
    ice_controller_ = std::make_unique<BasicIceController>(args);
  }
}

P2PTransportChannel::~P2PTransportChannel() {
  std::vector<Connection*> copy(connections().begin(), connections().end());
  for (Connection* con : copy) {
    con->Destroy();
  }
  for (auto& p : resolvers_) {
    p.resolver_->Destroy(false);
  }
  resolvers_.clear();
  RTC_DCHECK_RUN_ON(network_thread_);
}

// Add the allocator session to our list so that we know which sessions
// are still active.
void P2PTransportChannel::AddAllocatorSession(
    std::unique_ptr<PortAllocatorSession> session) {
  RTC_DCHECK_RUN_ON(network_thread_);

  session->set_generation(static_cast<uint32_t>(allocator_sessions_.size()));
  session->SignalPortReady.connect(this, &P2PTransportChannel::OnPortReady);
  session->SignalPortsPruned.connect(this, &P2PTransportChannel::OnPortsPruned);
  session->SignalCandidatesReady.connect(
      this, &P2PTransportChannel::OnCandidatesReady);
  session->SignalCandidateError.connect(this,
                                        &P2PTransportChannel::OnCandidateError);
  session->SignalCandidatesRemoved.connect(
      this, &P2PTransportChannel::OnCandidatesRemoved);
  session->SignalCandidatesAllocationDone.connect(
      this, &P2PTransportChannel::OnCandidatesAllocationDone);
  if (!allocator_sessions_.empty()) {
    allocator_session()->PruneAllPorts();
  }
  allocator_sessions_.push_back(std::move(session));
  regathering_controller_->set_allocator_session(allocator_session());

  // We now only want to apply new candidates that we receive to the ports
  // created by this new session because these are replacing those of the
  // previous sessions.
  PruneAllPorts();
}

void P2PTransportChannel::AddConnection(Connection* connection) {
  RTC_DCHECK_RUN_ON(network_thread_);
  connection->set_remote_ice_mode(remote_ice_mode_);
  connection->set_receiving_timeout(config_.receiving_timeout);
  connection->set_unwritable_timeout(config_.ice_unwritable_timeout);
  connection->set_unwritable_min_checks(config_.ice_unwritable_min_checks);
  connection->set_inactive_timeout(config_.ice_inactive_timeout);
  connection->SignalReadPacket.connect(this,
                                       &P2PTransportChannel::OnReadPacket);
  connection->SignalReadyToSend.connect(this,
                                        &P2PTransportChannel::OnReadyToSend);
  connection->SignalStateChange.connect(
      this, &P2PTransportChannel::OnConnectionStateChange);
  connection->SignalDestroyed.connect(
      this, &P2PTransportChannel::OnConnectionDestroyed);
  connection->SignalNominated.connect(this, &P2PTransportChannel::OnNominated);

  had_connection_ = true;

  connection->set_ice_event_log(&ice_event_log_);
  LogCandidatePairConfig(connection,
                         webrtc::IceCandidatePairConfigType::kAdded);

  ice_controller_->AddConnection(connection);
}

bool P2PTransportChannel::MaybeSwitchSelectedConnection(
    Connection* new_connection,
    IceControllerEvent reason) {
  RTC_DCHECK_RUN_ON(network_thread_);

  return MaybeSwitchSelectedConnection(
      reason, ice_controller_->ShouldSwitchConnection(reason, new_connection));
}

bool P2PTransportChannel::MaybeSwitchSelectedConnection(
    IceControllerEvent reason,
    IceControllerInterface::SwitchResult result) {
  RTC_DCHECK_RUN_ON(network_thread_);
  if (result.connection.has_value()) {
    RTC_LOG(LS_INFO) << "Switching selected connection due to: "
                     << reason.ToString();
    SwitchSelectedConnection(const_cast<Connection*>(*result.connection),
                             reason);
  }

  if (result.recheck_delay_ms.has_value()) {
    // If we do not switch to the connection because it missed the receiving
    // threshold, the new connection is in a better receiving state than the
    // currently selected connection. So we need to re-check whether it needs
    // to be switched at a later time.
    reason.dampening_delay = *result.recheck_delay_ms;
    invoker_.AsyncInvokeDelayed<void>(
        RTC_FROM_HERE, thread(),
        rtc::Bind(&P2PTransportChannel::SortConnectionsAndUpdateState, this,
                  reason),
        reason.dampening_delay);
  }

  return result.connection.has_value();
}

void P2PTransportChannel::SetIceRole(IceRole ice_role) {
  RTC_DCHECK_RUN_ON(network_thread_);
  if (ice_role_ != ice_role) {
    ice_role_ = ice_role;
    for (PortInterface* port : ports_) {
      port->SetIceRole(ice_role);
    }
    // Update role on pruned ports as well, because they may still have
    // connections alive that should be using the correct role.
    for (PortInterface* port : pruned_ports_) {
      port->SetIceRole(ice_role);
    }
  }
}

IceRole P2PTransportChannel::GetIceRole() const {
  RTC_DCHECK_RUN_ON(network_thread_);
  return ice_role_;
}

void P2PTransportChannel::SetIceTiebreaker(uint64_t tiebreaker) {
  RTC_DCHECK_RUN_ON(network_thread_);
  if (!ports_.empty() || !pruned_ports_.empty()) {
    RTC_LOG(LS_ERROR)
        << "Attempt to change tiebreaker after Port has been allocated.";
    return;
  }

  tiebreaker_ = tiebreaker;
}

IceTransportState P2PTransportChannel::GetState() const {
  RTC_DCHECK_RUN_ON(network_thread_);
  return state_;
}

webrtc::IceTransportState P2PTransportChannel::GetIceTransportState() const {
  RTC_DCHECK_RUN_ON(network_thread_);
  return standardized_state_;
}

const std::string& P2PTransportChannel::transport_name() const {
  RTC_DCHECK_RUN_ON(network_thread_);
  return transport_name_;
}

int P2PTransportChannel::component() const {
  RTC_DCHECK_RUN_ON(network_thread_);
  return component_;
}

bool P2PTransportChannel::writable() const {
  RTC_DCHECK_RUN_ON(network_thread_);
  return writable_;
}

bool P2PTransportChannel::receiving() const {
  RTC_DCHECK_RUN_ON(network_thread_);
  return receiving_;
}

IceGatheringState P2PTransportChannel::gathering_state() const {
  RTC_DCHECK_RUN_ON(network_thread_);
  return gathering_state_;
}

absl::optional<int> P2PTransportChannel::GetRttEstimate() {
  RTC_DCHECK_RUN_ON(network_thread_);
  if (selected_connection_ != nullptr &&
      selected_connection_->rtt_samples() > 0) {
    return selected_connection_->rtt();
  } else {
    return absl::nullopt;
  }
}

absl::optional<const CandidatePair>
P2PTransportChannel::GetSelectedCandidatePair() const {
  RTC_DCHECK_RUN_ON(network_thread_);
  if (selected_connection_ == nullptr) {
    return absl::nullopt;
  }

  CandidatePair pair;
  pair.local = SanitizeLocalCandidate(selected_connection_->local_candidate());
  pair.remote =
      SanitizeRemoteCandidate(selected_connection_->remote_candidate());
  return pair;
}

// A channel is considered ICE completed once there is at most one active
// connection per network and at least one active connection.
IceTransportState P2PTransportChannel::ComputeState() const {
  RTC_DCHECK_RUN_ON(network_thread_);
  if (!had_connection_) {
    return IceTransportState::STATE_INIT;
  }

  std::vector<Connection*> active_connections;
  for (Connection* connection : connections()) {
    if (connection->active()) {
      active_connections.push_back(connection);
    }
  }
  if (active_connections.empty()) {
    return IceTransportState::STATE_FAILED;
  }

  std::set<rtc::Network*> networks;
  for (Connection* connection : active_connections) {
    rtc::Network* network = connection->port()->Network();
    if (networks.find(network) == networks.end()) {
      networks.insert(network);
    } else {
      RTC_LOG(LS_VERBOSE) << ToString()
                          << ": Ice not completed yet for this channel as "
                          << network->ToString()
                          << " has more than 1 connection.";
      return IceTransportState::STATE_CONNECTING;
    }
  }

  ice_event_log_.DumpCandidatePairDescriptionToMemoryAsConfigEvents();
  return IceTransportState::STATE_COMPLETED;
}

// Compute the current RTCIceTransportState as described in
// https://www.w3.org/TR/webrtc/#dom-rtcicetransportstate
// TODO(bugs.webrtc.org/9218): Start signaling kCompleted once we have
// implemented end-of-candidates signalling.
webrtc::IceTransportState P2PTransportChannel::ComputeIceTransportState()
    const {
  RTC_DCHECK_RUN_ON(network_thread_);
  bool has_connection = false;
  for (Connection* connection : connections()) {
    if (connection->active()) {
      has_connection = true;
      break;
    }
  }

  if (had_connection_ && !has_connection) {
    return webrtc::IceTransportState::kFailed;
  }

  if (!writable() && has_been_writable_) {
    return webrtc::IceTransportState::kDisconnected;
  }

  if (!had_connection_ && !has_connection) {
    return webrtc::IceTransportState::kNew;
  }

  if (has_connection && !writable()) {
    // A candidate pair has been formed by adding a remote candidate
    // and gathering a local candidate.
    return webrtc::IceTransportState::kChecking;
  }

  return webrtc::IceTransportState::kConnected;
}

void P2PTransportChannel::SetIceParameters(const IceParameters& ice_params) {
  RTC_DCHECK_RUN_ON(network_thread_);
  RTC_LOG(LS_INFO) << "Set ICE ufrag: " << ice_params.ufrag
                   << " pwd: " << ice_params.pwd << " on transport "
                   << transport_name();
  ice_parameters_ = ice_params;
  // Note: Candidate gathering will restart when MaybeStartGathering is next
  // called.
}

void P2PTransportChannel::SetRemoteIceParameters(
    const IceParameters& ice_params) {
  RTC_DCHECK_RUN_ON(network_thread_);
  RTC_LOG(LS_INFO) << "Received remote ICE parameters: ufrag="
                   << ice_params.ufrag << ", renomination "
                   << (ice_params.renomination ? "enabled" : "disabled");
  IceParameters* current_ice = remote_ice();
  if (!current_ice || *current_ice != ice_params) {
    // Keep the ICE credentials so that newer connections
    // are prioritized over the older ones.
    remote_ice_parameters_.push_back(ice_params);
  }

  // Update the pwd of remote candidate if needed.
  for (RemoteCandidate& candidate : remote_candidates_) {
    if (candidate.username() == ice_params.ufrag &&
        candidate.password().empty()) {
      candidate.set_password(ice_params.pwd);
    }
  }
  // We need to update the credentials and generation for any peer reflexive
  // candidates.
  for (Connection* conn : connections()) {
    conn->MaybeSetRemoteIceParametersAndGeneration(
        ice_params, static_cast<int>(remote_ice_parameters_.size() - 1));
  }
  // Updating the remote ICE candidate generation could change the sort order.
  RequestSortAndStateUpdate(
      IceControllerEvent::REMOTE_CANDIDATE_GENERATION_CHANGE);
}

void P2PTransportChannel::SetRemoteIceMode(IceMode mode) {
  RTC_DCHECK_RUN_ON(network_thread_);
  remote_ice_mode_ = mode;
}

// TODO(qingsi): We apply the convention that setting a absl::optional parameter
// to null restores its default value in the implementation. However, some
// absl::optional parameters are only processed below if non-null, e.g.,
// regather_on_failed_networks_interval, and thus there is no way to restore the
// defaults. Fix this issue later for consistency.
void P2PTransportChannel::SetIceConfig(const IceConfig& config) {
  RTC_DCHECK_RUN_ON(network_thread_);
  if (config_.continual_gathering_policy != config.continual_gathering_policy) {
    if (!allocator_sessions_.empty()) {
      RTC_LOG(LS_ERROR) << "Trying to change continual gathering policy "
                           "when gathering has already started!";
    } else {
      config_.continual_gathering_policy = config.continual_gathering_policy;
      RTC_LOG(LS_INFO) << "Set continual_gathering_policy to "
                       << config_.continual_gathering_policy;
    }
  }

  if (config_.backup_connection_ping_interval !=
      config.backup_connection_ping_interval) {
    config_.backup_connection_ping_interval =
        config.backup_connection_ping_interval;
    RTC_LOG(LS_INFO) << "Set backup connection ping interval to "
                     << config_.backup_connection_ping_interval_or_default()
                     << " milliseconds.";
  }
  if (config_.receiving_timeout != config.receiving_timeout) {
    config_.receiving_timeout = config.receiving_timeout;
    for (Connection* connection : connections()) {
      connection->set_receiving_timeout(config_.receiving_timeout);
    }
    RTC_LOG(LS_INFO) << "Set ICE receiving timeout to "
                     << config_.receiving_timeout_or_default()
                     << " milliseconds";
  }

  config_.prioritize_most_likely_candidate_pairs =
      config.prioritize_most_likely_candidate_pairs;
  RTC_LOG(LS_INFO) << "Set ping most likely connection to "
                   << config_.prioritize_most_likely_candidate_pairs;

  if (config_.stable_writable_connection_ping_interval !=
      config.stable_writable_connection_ping_interval) {
    config_.stable_writable_connection_ping_interval =
        config.stable_writable_connection_ping_interval;
    RTC_LOG(LS_INFO)
        << "Set stable_writable_connection_ping_interval to "
        << config_.stable_writable_connection_ping_interval_or_default();
  }

  if (config_.presume_writable_when_fully_relayed !=
      config.presume_writable_when_fully_relayed) {
    if (!connections().empty()) {
      RTC_LOG(LS_ERROR) << "Trying to change 'presume writable' "
                           "while connections already exist!";
    } else {
      config_.presume_writable_when_fully_relayed =
          config.presume_writable_when_fully_relayed;
      RTC_LOG(LS_INFO) << "Set presume writable when fully relayed to "
                       << config_.presume_writable_when_fully_relayed;
    }
  }

  config_.surface_ice_candidates_on_ice_transport_type_changed =
      config.surface_ice_candidates_on_ice_transport_type_changed;
  if (config_.surface_ice_candidates_on_ice_transport_type_changed &&
      config_.continual_gathering_policy != GATHER_CONTINUALLY) {
    RTC_LOG(LS_WARNING)
        << "surface_ice_candidates_on_ice_transport_type_changed is "
           "ineffective since we do not gather continually.";
  }

  if (config_.regather_on_failed_networks_interval !=
      config.regather_on_failed_networks_interval) {
    config_.regather_on_failed_networks_interval =
        config.regather_on_failed_networks_interval;
    RTC_LOG(LS_INFO)
        << "Set regather_on_failed_networks_interval to "
        << config_.regather_on_failed_networks_interval_or_default();
  }

  if (config_.regather_all_networks_interval_range !=
      config.regather_all_networks_interval_range) {
    // Config validation is assumed to have already happened at the API layer.
    RTC_DCHECK(config.continual_gathering_policy != GATHER_ONCE);
    config_.regather_all_networks_interval_range =
        config.regather_all_networks_interval_range;
    RTC_LOG(LS_INFO) << "Set regather_all_networks_interval_range to "
                     << config.regather_all_networks_interval_range
                            .value_or(rtc::IntervalRange(-1, 0))
                            .ToString();
  }

  if (config_.receiving_switching_delay != config.receiving_switching_delay) {
    config_.receiving_switching_delay = config.receiving_switching_delay;
    RTC_LOG(LS_INFO) << "Set receiving_switching_delay to "
                     << config_.receiving_switching_delay_or_default();
  }

  if (config_.default_nomination_mode != config.default_nomination_mode) {
    config_.default_nomination_mode = config.default_nomination_mode;
    RTC_LOG(LS_INFO) << "Set default nomination mode to "
                     << static_cast<int>(config_.default_nomination_mode);
  }

  if (config_.ice_check_interval_strong_connectivity !=
      config.ice_check_interval_strong_connectivity) {
    config_.ice_check_interval_strong_connectivity =
        config.ice_check_interval_strong_connectivity;
    RTC_LOG(LS_INFO)
        << "Set strong ping interval to "
        << config_.ice_check_interval_strong_connectivity_or_default();
  }

  if (config_.ice_check_interval_weak_connectivity !=
      config.ice_check_interval_weak_connectivity) {
    config_.ice_check_interval_weak_connectivity =
        config.ice_check_interval_weak_connectivity;
    RTC_LOG(LS_INFO)
        << "Set weak ping interval to "
        << config_.ice_check_interval_weak_connectivity_or_default();
  }

  if (config_.ice_check_min_interval != config.ice_check_min_interval) {
    config_.ice_check_min_interval = config.ice_check_min_interval;
    RTC_LOG(LS_INFO) << "Set min ping interval to "
                     << config_.ice_check_min_interval_or_default();
  }

  if (config_.ice_unwritable_timeout != config.ice_unwritable_timeout) {
    config_.ice_unwritable_timeout = config.ice_unwritable_timeout;
    for (Connection* conn : connections()) {
      conn->set_unwritable_timeout(config_.ice_unwritable_timeout);
    }
    RTC_LOG(LS_INFO) << "Set unwritable timeout to "
                     << config_.ice_unwritable_timeout_or_default();
  }

  if (config_.ice_unwritable_min_checks != config.ice_unwritable_min_checks) {
    config_.ice_unwritable_min_checks = config.ice_unwritable_min_checks;
    for (Connection* conn : connections()) {
      conn->set_unwritable_min_checks(config_.ice_unwritable_min_checks);
    }
    RTC_LOG(LS_INFO) << "Set unwritable min checks to "
                     << config_.ice_unwritable_min_checks_or_default();
  }

  if (config_.ice_inactive_timeout != config.ice_inactive_timeout) {
    config_.ice_inactive_timeout = config.ice_inactive_timeout;
    for (Connection* conn : connections()) {
      conn->set_inactive_timeout(config_.ice_inactive_timeout);
    }
    RTC_LOG(LS_INFO) << "Set inactive timeout to "
                     << config_.ice_inactive_timeout_or_default();
  }

  if (config_.network_preference != config.network_preference) {
    config_.network_preference = config.network_preference;
    RequestSortAndStateUpdate(IceControllerEvent::NETWORK_PREFERENCE_CHANGE);
    RTC_LOG(LS_INFO) << "Set network preference to "
                     << (config_.network_preference.has_value()
                             ? config_.network_preference.value()
                             : -1);  // network_preference cannot be bound to
                                     // int with value_or.
  }

  // TODO(qingsi): Resolve the naming conflict of stun_keepalive_delay in
  // UDPPort and stun_keepalive_interval.
  if (config_.stun_keepalive_interval != config.stun_keepalive_interval) {
    config_.stun_keepalive_interval = config.stun_keepalive_interval;
    allocator_session()->SetStunKeepaliveIntervalForReadyPorts(
        config_.stun_keepalive_interval);
    RTC_LOG(LS_INFO) << "Set STUN keepalive interval to "
                     << config.stun_keepalive_interval_or_default();
  }

  if (webrtc::field_trial::IsEnabled("WebRTC-ExtraICEPing")) {
    RTC_LOG(LS_INFO) << "Set WebRTC-ExtraICEPing: Enabled";
  }
  if (webrtc::field_trial::IsEnabled("WebRTC-TurnAddMultiMapping")) {
    RTC_LOG(LS_INFO) << "Set WebRTC-TurnAddMultiMapping: Enabled";
  }

  webrtc::StructParametersParser::Create(
      "skip_relay_to_non_relay_connections",
      &field_trials_.skip_relay_to_non_relay_connections,
      "max_outstanding_pings", &field_trials_.max_outstanding_pings,
      "initial_select_dampening", &field_trials_.initial_select_dampening,
      "initial_select_dampening_ping_received",
      &field_trials_.initial_select_dampening_ping_received)
      ->Parse(webrtc::field_trial::FindFullName("WebRTC-IceFieldTrials"));

  if (field_trials_.skip_relay_to_non_relay_connections) {
    RTC_LOG(LS_INFO) << "Set skip_relay_to_non_relay_connections";
  }

  if (field_trials_.max_outstanding_pings.has_value()) {
    RTC_LOG(LS_INFO) << "Set max_outstanding_pings: "
                     << *field_trials_.max_outstanding_pings;
  }

  if (field_trials_.initial_select_dampening.has_value()) {
    RTC_LOG(LS_INFO) << "Set initial_select_dampening: "
                     << *field_trials_.initial_select_dampening;
  }

  if (field_trials_.initial_select_dampening_ping_received.has_value()) {
    RTC_LOG(LS_INFO) << "Set initial_select_dampening_ping_received: "
                     << *field_trials_.initial_select_dampening_ping_received;
  }

  webrtc::BasicRegatheringController::Config regathering_config(
      config_.regather_all_networks_interval_range,
      config_.regather_on_failed_networks_interval_or_default());
  regathering_controller_->SetConfig(regathering_config);

  ice_controller_->SetIceConfig(config_);

  RTC_DCHECK(ValidateIceConfig(config_).ok());
}

const IceConfig& P2PTransportChannel::config() const {
  RTC_DCHECK_RUN_ON(network_thread_);
  return config_;
}

// TODO(qingsi): Add tests for the config validation starting from
// PeerConnection::SetConfiguration.
// Static
RTCError P2PTransportChannel::ValidateIceConfig(const IceConfig& config) {
  if (config.regather_all_networks_interval_range &&
      config.continual_gathering_policy == GATHER_ONCE) {
    return RTCError(RTCErrorType::INVALID_PARAMETER,
                    "regather_all_networks_interval_range specified but "
                    "continual gathering policy is GATHER_ONCE");
  }

  if (config.ice_check_interval_strong_connectivity_or_default() <
      config.ice_check_interval_weak_connectivity.value_or(
          GetWeakPingIntervalInFieldTrial())) {
    return RTCError(RTCErrorType::INVALID_PARAMETER,
                    "Ping interval of candidate pairs is shorter when ICE is "
                    "strongly connected than that when ICE is weakly "
                    "connected");
  }

  if (config.receiving_timeout_or_default() <
      std::max(config.ice_check_interval_strong_connectivity_or_default(),
               config.ice_check_min_interval_or_default())) {
    return RTCError(
        RTCErrorType::INVALID_PARAMETER,
        "Receiving timeout is shorter than the minimal ping interval.");
  }

  if (config.backup_connection_ping_interval_or_default() <
      config.ice_check_interval_strong_connectivity_or_default()) {
    return RTCError(RTCErrorType::INVALID_PARAMETER,
                    "Ping interval of backup candidate pairs is shorter than "
                    "that of general candidate pairs when ICE is strongly "
                    "connected");
  }

  if (config.stable_writable_connection_ping_interval_or_default() <
      config.ice_check_interval_strong_connectivity_or_default()) {
    return RTCError(RTCErrorType::INVALID_PARAMETER,
                    "Ping interval of stable and writable candidate pairs is "
                    "shorter than that of general candidate pairs when ICE is "
                    "strongly connected");
  }

  if (config.ice_unwritable_timeout_or_default() >
      config.ice_inactive_timeout_or_default()) {
    return RTCError(RTCErrorType::INVALID_PARAMETER,
                    "The timeout period for the writability state to become "
                    "UNRELIABLE is longer than that to become TIMEOUT.");
  }

  if (config.regather_all_networks_interval_range &&
      config.regather_all_networks_interval_range.value().min() < 0) {
    return RTCError(
        RTCErrorType::INVALID_RANGE,
        "The minimum regathering interval for all networks is negative.");
  }

  return RTCError::OK();
}

const Connection* P2PTransportChannel::selected_connection() const {
  RTC_DCHECK_RUN_ON(network_thread_);
  return selected_connection_;
}

int P2PTransportChannel::check_receiving_interval() const {
  RTC_DCHECK_RUN_ON(network_thread_);
  return std::max(MIN_CHECK_RECEIVING_INTERVAL,
                  config_.receiving_timeout_or_default() / 10);
}

void P2PTransportChannel::MaybeStartGathering() {
  RTC_DCHECK_RUN_ON(network_thread_);
  if (ice_parameters_.ufrag.empty() || ice_parameters_.pwd.empty()) {
    RTC_LOG(LS_ERROR)
        << "Cannot gather candidates because ICE parameters are empty"
           " ufrag: "
        << ice_parameters_.ufrag << " pwd: " << ice_parameters_.pwd;
    return;
  }
  // Start gathering if we never started before, or if an ICE restart occurred.
  if (allocator_sessions_.empty() ||
      IceCredentialsChanged(allocator_sessions_.back()->ice_ufrag(),
                            allocator_sessions_.back()->ice_pwd(),
                            ice_parameters_.ufrag, ice_parameters_.pwd)) {
    if (gathering_state_ != kIceGatheringGathering) {
      gathering_state_ = kIceGatheringGathering;
      SignalGatheringState(this);
    }

    if (!allocator_sessions_.empty()) {
      IceRestartState state;
      if (writable()) {
        state = IceRestartState::CONNECTED;
      } else if (IsGettingPorts()) {
        state = IceRestartState::CONNECTING;
      } else {
        state = IceRestartState::DISCONNECTED;
      }
      RTC_HISTOGRAM_ENUMERATION("WebRTC.PeerConnection.IceRestartState",
                                static_cast<int>(state),
                                static_cast<int>(IceRestartState::MAX_VALUE));
    }

    // Time for a new allocator.
    std::unique_ptr<PortAllocatorSession> pooled_session =
        allocator_->TakePooledSession(transport_name(), component(),
                                      ice_parameters_.ufrag,
                                      ice_parameters_.pwd);
    if (pooled_session) {
      AddAllocatorSession(std::move(pooled_session));
      PortAllocatorSession* raw_pooled_session =
          allocator_sessions_.back().get();
      // Process the pooled session's existing candidates/ports, if they exist.
      OnCandidatesReady(raw_pooled_session,
                        raw_pooled_session->ReadyCandidates());
      for (PortInterface* port : allocator_sessions_.back()->ReadyPorts()) {
        OnPortReady(raw_pooled_session, port);
      }
      if (allocator_sessions_.back()->CandidatesAllocationDone()) {
        OnCandidatesAllocationDone(raw_pooled_session);
      }
    } else {
      AddAllocatorSession(allocator_->CreateSession(
          transport_name(), component(), ice_parameters_.ufrag,
          ice_parameters_.pwd));
      allocator_sessions_.back()->StartGettingPorts();
    }
  }
}

// A new port is available, attempt to make connections for it
void P2PTransportChannel::OnPortReady(PortAllocatorSession* session,
                                      PortInterface* port) {
  RTC_DCHECK_RUN_ON(network_thread_);

  // Set in-effect options on the new port
  for (OptionMap::const_iterator it = options_.begin(); it != options_.end();
       ++it) {
    int val = port->SetOption(it->first, it->second);
    if (val < 0) {
      // Errors are frequent, so use LS_INFO. bugs.webrtc.org/9221
      RTC_LOG(LS_INFO) << port->ToString() << ": SetOption(" << it->first
                       << ", " << it->second
                       << ") failed: " << port->GetError();
    }
  }

  // Remember the ports and candidates, and signal that candidates are ready.
  // The session will handle this, and send an initiate/accept/modify message
  // if one is pending.

  port->SetIceRole(ice_role_);
  port->SetIceTiebreaker(tiebreaker_);
  ports_.push_back(port);
  port->SignalUnknownAddress.connect(this,
                                     &P2PTransportChannel::OnUnknownAddress);
  port->SignalDestroyed.connect(this, &P2PTransportChannel::OnPortDestroyed);

  port->SignalRoleConflict.connect(this, &P2PTransportChannel::OnRoleConflict);
  port->SignalSentPacket.connect(this, &P2PTransportChannel::OnSentPacket);

  // Attempt to create a connection from this new port to all of the remote
  // candidates that we were given so far.

  std::vector<RemoteCandidate>::iterator iter;
  for (iter = remote_candidates_.begin(); iter != remote_candidates_.end();
       ++iter) {
    CreateConnection(port, *iter, iter->origin_port());
  }

  SortConnectionsAndUpdateState(
      IceControllerEvent::NEW_CONNECTION_FROM_LOCAL_CANDIDATE);
}

// A new candidate is available, let listeners know
void P2PTransportChannel::OnCandidatesReady(
    PortAllocatorSession* session,
    const std::vector<Candidate>& candidates) {
  RTC_DCHECK_RUN_ON(network_thread_);
  for (size_t i = 0; i < candidates.size(); ++i) {
    SignalCandidateGathered(this, candidates[i]);
  }
}

void P2PTransportChannel::OnCandidateError(
    PortAllocatorSession* session,
    const IceCandidateErrorEvent& event) {
  RTC_DCHECK(network_thread_ == rtc::Thread::Current());
  SignalCandidateError(this, event);
}

void P2PTransportChannel::OnCandidatesAllocationDone(
    PortAllocatorSession* session) {
  RTC_DCHECK_RUN_ON(network_thread_);
  if (config_.gather_continually()) {
    RTC_LOG(LS_INFO) << "P2PTransportChannel: " << transport_name()
                     << ", component " << component()
                     << " gathering complete, but using continual "
                        "gathering so not changing gathering state.";
    return;
  }
  gathering_state_ = kIceGatheringComplete;
  RTC_LOG(LS_INFO) << "P2PTransportChannel: " << transport_name()
                   << ", component " << component() << " gathering complete";
  SignalGatheringState(this);
}

// Handle stun packets
void P2PTransportChannel::OnUnknownAddress(PortInterface* port,
                                           const rtc::SocketAddress& address,
                                           ProtocolType proto,
                                           IceMessage* stun_msg,
                                           const std::string& remote_username,
                                           bool port_muxed) {
  RTC_DCHECK_RUN_ON(network_thread_);

  // Port has received a valid stun packet from an address that no Connection
  // is currently available for. See if we already have a candidate with the
  // address. If it isn't we need to create new candidate for it.
  //
  // TODO(qingsi): There is a caveat of the logic below if we have remote
  // candidates with hostnames. We could create a prflx candidate that is
  // identical to a host candidate that are currently in the process of name
  // resolution. We would not have a duplicate candidate since when adding the
  // resolved host candidate, FinishingAddingRemoteCandidate does
  // MaybeUpdatePeerReflexiveCandidate, and the prflx candidate would be updated
  // to a host candidate. As a result, for a brief moment we would have a prflx
  // candidate showing a private IP address, though we do not signal prflx
  // candidates to applications and we could obfuscate the IP addresses of prflx
  // candidates in P2PTransportChannel::GetStats. The difficulty of preventing
  // creating the prflx from the beginning is that we do not have a reliable way
  // to claim two candidates are identical without the address information. If
  // we always pause the addition of a prflx candidate when there is ongoing
  // name resolution and dedup after we have a resolved address, we run into the
  // risk of losing/delaying the addition of a non-identical candidate that
  // could be the only way to have a connection, if the resolution never
  // completes or is significantly delayed.
  const Candidate* candidate = nullptr;
  for (const Candidate& c : remote_candidates_) {
    if (c.username() == remote_username && c.address() == address &&
        c.protocol() == ProtoToString(proto)) {
      candidate = &c;
      break;
    }
  }

  uint32_t remote_generation = 0;
  std::string remote_password;
  // The STUN binding request may arrive after setRemoteDescription and before
  // adding remote candidate, so we need to set the password to the shared
  // password and set the generation if the user name matches.
  const IceParameters* ice_param =
      FindRemoteIceFromUfrag(remote_username, &remote_generation);
  // Note: if not found, the remote_generation will still be 0.
  if (ice_param != nullptr) {
    remote_password = ice_param->pwd;
  }

  Candidate remote_candidate;
  bool remote_candidate_is_new = (candidate == nullptr);
  if (!remote_candidate_is_new) {
    remote_candidate = *candidate;
  } else {
    // Create a new candidate with this address.
    // The priority of the candidate is set to the PRIORITY attribute
    // from the request.
    const StunUInt32Attribute* priority_attr =
        stun_msg->GetUInt32(STUN_ATTR_PRIORITY);
    if (!priority_attr) {
      RTC_LOG(LS_WARNING) << "P2PTransportChannel::OnUnknownAddress - "
                             "No STUN_ATTR_PRIORITY found in the "
                             "stun request message";
      port->SendBindingErrorResponse(stun_msg, address, STUN_ERROR_BAD_REQUEST,
                                     STUN_ERROR_REASON_BAD_REQUEST);
      return;
    }
    int remote_candidate_priority = priority_attr->value();

    uint16_t network_id = 0;
    uint16_t network_cost = 0;
    const StunUInt32Attribute* network_attr =
        stun_msg->GetUInt32(STUN_ATTR_NETWORK_INFO);
    if (network_attr) {
      uint32_t network_info = network_attr->value();
      network_id = static_cast<uint16_t>(network_info >> 16);
      network_cost = static_cast<uint16_t>(network_info);
    }

    // RFC 5245
    // If the source transport address of the request does not match any
    // existing remote candidates, it represents a new peer reflexive remote
    // candidate.
    remote_candidate = Candidate(
        component(), ProtoToString(proto), address, remote_candidate_priority,
        remote_username, remote_password, PRFLX_PORT_TYPE, remote_generation,
        "", network_id, network_cost);

    // From RFC 5245, section-7.2.1.3:
    // The foundation of the candidate is set to an arbitrary value, different
    // from the foundation for all other remote candidates.
    remote_candidate.set_foundation(
        rtc::ToString(rtc::ComputeCrc32(remote_candidate.id())));
  }

  // RFC5245, the agent constructs a pair whose local candidate is equal to
  // the transport address on which the STUN request was received, and a
  // remote candidate equal to the source transport address where the
  // request came from.

  // There shouldn't be an existing connection with this remote address.
  // When ports are muxed, this channel might get multiple unknown address
  // signals. In that case if the connection is already exists, we should
  // simply ignore the signal otherwise send server error.
  if (port->GetConnection(remote_candidate.address())) {
    if (port_muxed) {
      RTC_LOG(LS_INFO) << "Connection already exists for peer reflexive "
                          "candidate: "
                       << remote_candidate.ToSensitiveString();
      return;
    } else {
      RTC_NOTREACHED();
      port->SendBindingErrorResponse(stun_msg, address, STUN_ERROR_SERVER_ERROR,
                                     STUN_ERROR_REASON_SERVER_ERROR);
      return;
    }
  }

  Connection* connection =
      port->CreateConnection(remote_candidate, PortInterface::ORIGIN_THIS_PORT);
  if (!connection) {
    // This could happen in some scenarios. For example, a TurnPort may have
    // had a refresh request timeout, so it won't create connections.
    port->SendBindingErrorResponse(stun_msg, address, STUN_ERROR_SERVER_ERROR,
                                   STUN_ERROR_REASON_SERVER_ERROR);
    return;
  }

  RTC_LOG(LS_INFO) << "Adding connection from "
                   << (remote_candidate_is_new ? "peer reflexive"
                                               : "resurrected")
                   << " candidate: " << remote_candidate.ToSensitiveString();
  AddConnection(connection);
  connection->HandleBindingRequest(stun_msg);

  // Update the list of connections since we just added another.  We do this
  // after sending the response since it could (in principle) delete the
  // connection in question.
  SortConnectionsAndUpdateState(
      IceControllerEvent::NEW_CONNECTION_FROM_UNKNOWN_REMOTE_ADDRESS);
}

void P2PTransportChannel::OnCandidateFilterChanged(uint32_t prev_filter,
                                                   uint32_t cur_filter) {
  RTC_DCHECK_RUN_ON(network_thread_);
  if (prev_filter == cur_filter || allocator_session() == nullptr) {
    return;
  }
  if (config_.surface_ice_candidates_on_ice_transport_type_changed) {
    allocator_session()->SetCandidateFilter(cur_filter);
  }
}

void P2PTransportChannel::OnRoleConflict(PortInterface* port) {
  SignalRoleConflict(this);  // STUN ping will be sent when SetRole is called
                             // from Transport.
}

const IceParameters* P2PTransportChannel::FindRemoteIceFromUfrag(
    const std::string& ufrag,
    uint32_t* generation) {
  RTC_DCHECK_RUN_ON(network_thread_);
  const auto& params = remote_ice_parameters_;
  auto it = std::find_if(
      params.rbegin(), params.rend(),
      [ufrag](const IceParameters& param) { return param.ufrag == ufrag; });
  if (it == params.rend()) {
    // Not found.
    return nullptr;
  }
  *generation = params.rend() - it - 1;
  return &(*it);
}

void P2PTransportChannel::OnNominated(Connection* conn) {
  RTC_DCHECK_RUN_ON(network_thread_);
  RTC_DCHECK(ice_role_ == ICEROLE_CONTROLLED);

  if (selected_connection_ == conn) {
    return;
  }

  // TODO(qingsi): RequestSortAndStateUpdate will eventually call
  // MaybeSwitchSelectedConnection again. Rewrite this logic.
  if (MaybeSwitchSelectedConnection(
          conn, IceControllerEvent::NOMINATION_ON_CONTROLLED_SIDE)) {
    // Now that we have selected a connection, it is time to prune other
    // connections and update the read/write state of the channel.
    RequestSortAndStateUpdate(
        IceControllerEvent::NOMINATION_ON_CONTROLLED_SIDE);
  } else {
    RTC_LOG(LS_INFO)
        << "Not switching the selected connection on controlled side yet: "
        << conn->ToString();
  }
}

void P2PTransportChannel::ResolveHostnameCandidate(const Candidate& candidate) {
  RTC_DCHECK_RUN_ON(network_thread_);
  if (!async_resolver_factory_) {
    RTC_LOG(LS_WARNING) << "Dropping ICE candidate with hostname address "
                        << "(no AsyncResolverFactory)";
    return;
  }

  rtc::AsyncResolverInterface* resolver = async_resolver_factory_->Create();
  resolvers_.emplace_back(candidate, resolver);
  resolver->SignalDone.connect(this, &P2PTransportChannel::OnCandidateResolved);
  resolver->Start(candidate.address());
  RTC_LOG(LS_INFO) << "Asynchronously resolving ICE candidate hostname "
                   << candidate.address().HostAsSensitiveURIString();
}

void P2PTransportChannel::AddRemoteCandidate(const Candidate& candidate) {
  RTC_DCHECK_RUN_ON(network_thread_);

  uint32_t generation = GetRemoteCandidateGeneration(candidate);
  // If a remote candidate with a previous generation arrives, drop it.
  if (generation < remote_ice_generation()) {
    RTC_LOG(LS_WARNING) << "Dropping a remote candidate because its ufrag "
                        << candidate.username()
                        << " indicates it was for a previous generation.";
    return;
  }

  Candidate new_remote_candidate(candidate);
  new_remote_candidate.set_generation(generation);
  // ICE candidates don't need to have username and password set, but
  // the code below this (specifically, ConnectionRequest::Prepare in
  // port.cc) uses the remote candidates's username.  So, we set it
  // here.
  if (remote_ice()) {
    if (candidate.username().empty()) {
      new_remote_candidate.set_username(remote_ice()->ufrag);
    }
    if (new_remote_candidate.username() == remote_ice()->ufrag) {
      if (candidate.password().empty()) {
        new_remote_candidate.set_password(remote_ice()->pwd);
      }
    } else {
      // The candidate belongs to the next generation. Its pwd will be set
      // when the new remote ICE credentials arrive.
      RTC_LOG(LS_WARNING)
          << "A remote candidate arrives with an unknown ufrag: "
          << candidate.username();
    }
  }

  if (new_remote_candidate.address().IsUnresolvedIP()) {
    ResolveHostnameCandidate(new_remote_candidate);
    return;
  }

  FinishAddingRemoteCandidate(new_remote_candidate);
}

P2PTransportChannel::CandidateAndResolver::CandidateAndResolver(
    const Candidate& candidate,
    rtc::AsyncResolverInterface* resolver)
    : candidate_(candidate), resolver_(resolver) {}

P2PTransportChannel::CandidateAndResolver::~CandidateAndResolver() {}

void P2PTransportChannel::OnCandidateResolved(
    rtc::AsyncResolverInterface* resolver) {
  RTC_DCHECK_RUN_ON(network_thread_);
  auto p =
      absl::c_find_if(resolvers_, [resolver](const CandidateAndResolver& cr) {
        return cr.resolver_ == resolver;
      });
  if (p == resolvers_.end()) {
    RTC_LOG(LS_ERROR) << "Unexpected AsyncResolver signal";
    RTC_NOTREACHED();
    return;
  }
  Candidate candidate = p->candidate_;
  resolvers_.erase(p);
  AddRemoteCandidateWithResolver(candidate, resolver);
  invoker_.AsyncInvoke<void>(
      RTC_FROM_HERE, thread(),
      rtc::Bind(&rtc::AsyncResolverInterface::Destroy, resolver, false));
}

void P2PTransportChannel::AddRemoteCandidateWithResolver(
    Candidate candidate,
    rtc::AsyncResolverInterface* resolver) {
  RTC_DCHECK_RUN_ON(network_thread_);
  if (resolver->GetError()) {
    RTC_LOG(LS_WARNING) << "Failed to resolve ICE candidate hostname "
                        << candidate.address().HostAsSensitiveURIString()
                        << " with error " << resolver->GetError();
    return;
  }

  rtc::SocketAddress resolved_address;
  // Prefer IPv6 to IPv4 if we have it (see RFC 5245 Section 15.1).
  // TODO(zstein): This won't work if we only have IPv4 locally but receive an
  // AAAA DNS record.
  bool have_address =
      resolver->GetResolvedAddress(AF_INET6, &resolved_address) ||
      resolver->GetResolvedAddress(AF_INET, &resolved_address);
  if (!have_address) {
    RTC_LOG(LS_INFO) << "ICE candidate hostname "
                     << candidate.address().HostAsSensitiveURIString()
                     << " could not be resolved";
    return;
  }

  RTC_LOG(LS_INFO) << "Resolved ICE candidate hostname "
                   << candidate.address().HostAsSensitiveURIString() << " to "
                   << resolved_address.ipaddr().ToSensitiveString();
  candidate.set_address(resolved_address);
  FinishAddingRemoteCandidate(candidate);
}

void P2PTransportChannel::FinishAddingRemoteCandidate(
    const Candidate& new_remote_candidate) {
  RTC_DCHECK_RUN_ON(network_thread_);
  // If this candidate matches what was thought to be a peer reflexive
  // candidate, we need to update the candidate priority/etc.
  for (Connection* conn : connections()) {
    conn->MaybeUpdatePeerReflexiveCandidate(new_remote_candidate);
  }

  // Create connections to this remote candidate.
  CreateConnections(new_remote_candidate, NULL);

  // Resort the connections list, which may have new elements.
  SortConnectionsAndUpdateState(
      IceControllerEvent::NEW_CONNECTION_FROM_REMOTE_CANDIDATE);
}

void P2PTransportChannel::RemoveRemoteCandidate(
    const Candidate& cand_to_remove) {
  RTC_DCHECK_RUN_ON(network_thread_);
  auto iter =
      std::remove_if(remote_candidates_.begin(), remote_candidates_.end(),
                     [cand_to_remove](const Candidate& candidate) {
                       return cand_to_remove.MatchesForRemoval(candidate);
                     });
  if (iter != remote_candidates_.end()) {
    RTC_LOG(LS_VERBOSE) << "Removed remote candidate "
                        << cand_to_remove.ToSensitiveString();
    remote_candidates_.erase(iter, remote_candidates_.end());
  }
}

void P2PTransportChannel::RemoveAllRemoteCandidates() {
  RTC_DCHECK_RUN_ON(network_thread_);
  remote_candidates_.clear();
}

// Creates connections from all of the ports that we care about to the given
// remote candidate.  The return value is true if we created a connection from
// the origin port.
bool P2PTransportChannel::CreateConnections(const Candidate& remote_candidate,
                                            PortInterface* origin_port) {
  RTC_DCHECK_RUN_ON(network_thread_);

  // If we've already seen the new remote candidate (in the current candidate
  // generation), then we shouldn't try creating connections for it.
  // We either already have a connection for it, or we previously created one
  // and then later pruned it. If we don't return, the channel will again
  // re-create any connections that were previously pruned, which will then
  // immediately be re-pruned, churning the network for no purpose.
  // This only applies to candidates received over signaling (i.e. origin_port
  // is NULL).
  if (!origin_port && IsDuplicateRemoteCandidate(remote_candidate)) {
    // return true to indicate success, without creating any new connections.
    return true;
  }

  // Add a new connection for this candidate to every port that allows such a
  // connection (i.e., if they have compatible protocols) and that does not
  // already have a connection to an equivalent candidate.  We must be careful
  // to make sure that the origin port is included, even if it was pruned,
  // since that may be the only port that can create this connection.
  bool created = false;
  std::vector<PortInterface*>::reverse_iterator it;
  for (it = ports_.rbegin(); it != ports_.rend(); ++it) {
    if (CreateConnection(*it, remote_candidate, origin_port)) {
      if (*it == origin_port)
        created = true;
    }
  }

  if ((origin_port != NULL) && !absl::c_linear_search(ports_, origin_port)) {
    if (CreateConnection(origin_port, remote_candidate, origin_port))
      created = true;
  }

  // Remember this remote candidate so that we can add it to future ports.
  RememberRemoteCandidate(remote_candidate, origin_port);

  return created;
}

// Setup a connection object for the local and remote candidate combination.
// And then listen to connection object for changes.
bool P2PTransportChannel::CreateConnection(PortInterface* port,
                                           const Candidate& remote_candidate,
                                           PortInterface* origin_port) {
  RTC_DCHECK_RUN_ON(network_thread_);
  if (!port->SupportsProtocol(remote_candidate.protocol())) {
    return false;
  }

  if (field_trials_.skip_relay_to_non_relay_connections) {
    if ((port->Type() != remote_candidate.type()) &&
        (port->Type() == RELAY_PORT_TYPE ||
         remote_candidate.type() == RELAY_PORT_TYPE)) {
      RTC_LOG(LS_INFO) << ToString() << ": skip creating connection "
                       << port->Type() << " to " << remote_candidate.type();
      return false;
    }
  }

  // Look for an existing connection with this remote address.  If one is not
  // found or it is found but the existing remote candidate has an older
  // generation, then we can create a new connection for this address.
  Connection* connection = port->GetConnection(remote_candidate.address());
  if (connection == nullptr || connection->remote_candidate().generation() <
                                   remote_candidate.generation()) {
    // Don't create a connection if this is a candidate we received in a
    // message and we are not allowed to make outgoing connections.
    PortInterface::CandidateOrigin origin = GetOrigin(port, origin_port);
    if (origin == PortInterface::ORIGIN_MESSAGE && incoming_only_) {
      return false;
    }
    Connection* connection = port->CreateConnection(remote_candidate, origin);
    if (!connection) {
      return false;
    }
    AddConnection(connection);
    RTC_LOG(LS_INFO) << ToString()
                     << ": Created connection with origin: " << origin
                     << ", total: " << connections().size();
    return true;
  }

  // No new connection was created.
  // It is not legal to try to change any of the parameters of an existing
  // connection; however, the other side can send a duplicate candidate.
  if (!remote_candidate.IsEquivalent(connection->remote_candidate())) {
    RTC_LOG(INFO) << "Attempt to change a remote candidate."
                     " Existing remote candidate: "
                  << connection->remote_candidate().ToSensitiveString()
                  << "New remote candidate: "
                  << remote_candidate.ToSensitiveString();
  }
  return false;
}

bool P2PTransportChannel::FindConnection(Connection* connection) const {
  RTC_DCHECK_RUN_ON(network_thread_);
  return absl::c_linear_search(connections(), connection);
}

uint32_t P2PTransportChannel::GetRemoteCandidateGeneration(
    const Candidate& candidate) {
  RTC_DCHECK_RUN_ON(network_thread_);
  // If the candidate has a ufrag, use it to find the generation.
  if (!candidate.username().empty()) {
    uint32_t generation = 0;
    if (!FindRemoteIceFromUfrag(candidate.username(), &generation)) {
      // If the ufrag is not found, assume the next/future generation.
      generation = static_cast<uint32_t>(remote_ice_parameters_.size());
    }
    return generation;
  }
  // If candidate generation is set, use that.
  if (candidate.generation() > 0) {
    return candidate.generation();
  }
  // Otherwise, assume the generation from remote ice parameters.
  return remote_ice_generation();
}

// Check if remote candidate is already cached.
bool P2PTransportChannel::IsDuplicateRemoteCandidate(
    const Candidate& candidate) {
  RTC_DCHECK_RUN_ON(network_thread_);
  for (size_t i = 0; i < remote_candidates_.size(); ++i) {
    if (remote_candidates_[i].IsEquivalent(candidate)) {
      return true;
    }
  }
  return false;
}

// Maintain our remote candidate list, adding this new remote one.
void P2PTransportChannel::RememberRemoteCandidate(
    const Candidate& remote_candidate,
    PortInterface* origin_port) {
  RTC_DCHECK_RUN_ON(network_thread_);
  // Remove any candidates whose generation is older than this one.  The
  // presence of a new generation indicates that the old ones are not useful.
  size_t i = 0;
  while (i < remote_candidates_.size()) {
    if (remote_candidates_[i].generation() < remote_candidate.generation()) {
      RTC_LOG(INFO) << "Pruning candidate from old generation: "
                    << remote_candidates_[i].address().ToSensitiveString();
      remote_candidates_.erase(remote_candidates_.begin() + i);
    } else {
      i += 1;
    }
  }

  // Make sure this candidate is not a duplicate.
  if (IsDuplicateRemoteCandidate(remote_candidate)) {
    RTC_LOG(INFO) << "Duplicate candidate: "
                  << remote_candidate.ToSensitiveString();
    return;
  }

  // Try this candidate for all future ports.
  remote_candidates_.push_back(RemoteCandidate(remote_candidate, origin_port));
}

// Set options on ourselves is simply setting options on all of our available
// port objects.
int P2PTransportChannel::SetOption(rtc::Socket::Option opt, int value) {
  RTC_DCHECK_RUN_ON(network_thread_);
  OptionMap::iterator it = options_.find(opt);
  if (it == options_.end()) {
    options_.insert(std::make_pair(opt, value));
  } else if (it->second == value) {
    return 0;
  } else {
    it->second = value;
  }

  for (PortInterface* port : ports_) {
    int val = port->SetOption(opt, value);
    if (val < 0) {
      // Because this also occurs deferred, probably no point in reporting an
      // error
      RTC_LOG(WARNING) << "SetOption(" << opt << ", " << value
                       << ") failed: " << port->GetError();
    }
  }
  return 0;
}

bool P2PTransportChannel::GetOption(rtc::Socket::Option opt, int* value) {
  RTC_DCHECK_RUN_ON(network_thread_);

  const auto& found = options_.find(opt);
  if (found == options_.end()) {
    return false;
  }
  *value = found->second;
  return true;
}

int P2PTransportChannel::GetError() {
  RTC_DCHECK_RUN_ON(network_thread_);
  return error_;
}

// Send data to the other side, using our selected connection.
int P2PTransportChannel::SendPacket(const char* data,
                                    size_t len,
                                    const rtc::PacketOptions& options,
                                    int flags) {
  RTC_DCHECK_RUN_ON(network_thread_);
  if (flags != 0) {
    error_ = EINVAL;
    return -1;
  }
  // If we don't think the connection is working yet, return ENOTCONN
  // instead of sending a packet that will probably be dropped.
  if (!ReadyToSend(selected_connection_)) {
    error_ = ENOTCONN;
    return -1;
  }

  last_sent_packet_id_ = options.packet_id;
  rtc::PacketOptions modified_options(options);
  modified_options.info_signaled_after_sent.packet_type =
      rtc::PacketType::kData;
  int sent = selected_connection_->Send(data, len, modified_options);
  if (sent <= 0) {
    RTC_DCHECK(sent < 0);
    error_ = selected_connection_->GetError();
  }
  return sent;
}

bool P2PTransportChannel::GetStats(IceTransportStats* ice_transport_stats) {
  RTC_DCHECK_RUN_ON(network_thread_);
  // Gather candidate and candidate pair stats.
  ice_transport_stats->candidate_stats_list.clear();
  ice_transport_stats->connection_infos.clear();

  if (!allocator_sessions_.empty()) {
    allocator_session()->GetCandidateStatsFromReadyPorts(
        &ice_transport_stats->candidate_stats_list);
  }

  // TODO(qingsi): Remove naming inconsistency for candidate pair/connection.
  for (Connection* connection : connections()) {
    ConnectionInfo stats = connection->stats();
    stats.local_candidate = SanitizeLocalCandidate(stats.local_candidate);
    stats.remote_candidate = SanitizeRemoteCandidate(stats.remote_candidate);
    stats.best_connection = (selected_connection_ == connection);
    ice_transport_stats->connection_infos.push_back(std::move(stats));
    connection->set_reported(true);
  }

  ice_transport_stats->selected_candidate_pair_changes =
      selected_candidate_pair_changes_;
  return true;
}

absl::optional<rtc::NetworkRoute> P2PTransportChannel::network_route() const {
  RTC_DCHECK_RUN_ON(network_thread_);
  return network_route_;
}

rtc::DiffServCodePoint P2PTransportChannel::DefaultDscpValue() const {
  RTC_DCHECK_RUN_ON(network_thread_);
  OptionMap::const_iterator it = options_.find(rtc::Socket::OPT_DSCP);
  if (it == options_.end()) {
    return rtc::DSCP_NO_CHANGE;
  }
  return static_cast<rtc::DiffServCodePoint>(it->second);
}

rtc::ArrayView<Connection*> P2PTransportChannel::connections() const {
  RTC_DCHECK_RUN_ON(network_thread_);
  rtc::ArrayView<const Connection*> res = ice_controller_->connections();
  return rtc::ArrayView<Connection*>(const_cast<Connection**>(res.data()),
                                     res.size());
}

// Monitor connection states.
void P2PTransportChannel::UpdateConnectionStates() {
  RTC_DCHECK_RUN_ON(network_thread_);
  int64_t now = rtc::TimeMillis();

  // We need to copy the list of connections since some may delete themselves
  // when we call UpdateState.
  for (Connection* c : connections()) {
    c->UpdateState(now);
  }
}

// Prepare for best candidate sorting.
void P2PTransportChannel::RequestSortAndStateUpdate(
    IceControllerEvent reason_to_sort) {
  RTC_DCHECK_RUN_ON(network_thread_);
  if (!sort_dirty_) {
    invoker_.AsyncInvoke<void>(
        RTC_FROM_HERE, thread(),
        rtc::Bind(&P2PTransportChannel::SortConnectionsAndUpdateState, this,
                  reason_to_sort));
    sort_dirty_ = true;
  }
}

void P2PTransportChannel::MaybeStartPinging() {
  RTC_DCHECK_RUN_ON(network_thread_);
  if (started_pinging_) {
    return;
  }

  if (ice_controller_->HasPingableConnection()) {
    RTC_LOG(LS_INFO) << ToString()
                     << ": Have a pingable connection for the first time; "
                        "starting to ping.";
    invoker_.AsyncInvoke<void>(
        RTC_FROM_HERE, thread(),
        rtc::Bind(&P2PTransportChannel::CheckAndPing, this));
    regathering_controller_->Start();
    started_pinging_ = true;
  }
}

bool P2PTransportChannel::IsPortPruned(const Port* port) const {
  RTC_DCHECK_RUN_ON(network_thread_);
  return !absl::c_linear_search(ports_, port);
}

bool P2PTransportChannel::IsRemoteCandidatePruned(const Candidate& cand) const {
  RTC_DCHECK_RUN_ON(network_thread_);
  return !absl::c_linear_search(remote_candidates_, cand);
}

bool P2PTransportChannel::PresumedWritable(const Connection* conn) const {
  RTC_DCHECK_RUN_ON(network_thread_);
  return (conn->write_state() == Connection::STATE_WRITE_INIT &&
          config_.presume_writable_when_fully_relayed &&
          conn->local_candidate().type() == RELAY_PORT_TYPE &&
          (conn->remote_candidate().type() == RELAY_PORT_TYPE ||
           conn->remote_candidate().type() == PRFLX_PORT_TYPE));
}

// Sort the available connections to find the best one.  We also monitor
// the number of available connections and the current state.
void P2PTransportChannel::SortConnectionsAndUpdateState(
    IceControllerEvent reason_to_sort) {
  RTC_DCHECK_RUN_ON(network_thread_);

  // Make sure the connection states are up-to-date since this affects how they
  // will be sorted.
  UpdateConnectionStates();

  // Any changes after this point will require a re-sort.
  sort_dirty_ = false;

  // If necessary, switch to the new choice. Note that |top_connection| doesn't
  // have to be writable to become the selected connection although it will
  // have higher priority if it is writable.
  MaybeSwitchSelectedConnection(
      reason_to_sort, ice_controller_->SortAndSwitchConnection(reason_to_sort));

  // The controlled side can prune only if the selected connection has been
  // nominated because otherwise it may prune the connection that will be
  // selected by the controlling side.
  // TODO(honghaiz): This is not enough to prevent a connection from being
  // pruned too early because with aggressive nomination, the controlling side
  // will nominate every connection until it becomes writable.
  if (ice_role_ == ICEROLE_CONTROLLING ||
      (selected_connection_ && selected_connection_->nominated())) {
    PruneConnections();
  }

  // Check if all connections are timedout.
  bool all_connections_timedout = true;
  for (const Connection* conn : connections()) {
    if (conn->write_state() != Connection::STATE_WRITE_TIMEOUT) {
      all_connections_timedout = false;
      break;
    }
  }

  // Now update the writable state of the channel with the information we have
  // so far.
  if (all_connections_timedout) {
    HandleAllTimedOut();
  }

  // Update the state of this channel.
  UpdateState();

  // Also possibly start pinging.
  // We could start pinging if:
  // * The first connection was created.
  // * ICE credentials were provided.
  // * A TCP connection became connected.
  MaybeStartPinging();
}

void P2PTransportChannel::PruneConnections() {
  RTC_DCHECK_RUN_ON(network_thread_);
  std::vector<const Connection*> connections_to_prune =
      ice_controller_->PruneConnections();
  for (const Connection* conn : connections_to_prune) {
    const_cast<Connection*>(conn)->Prune();
  }
}

// Change the selected connection, and let listeners know.
void P2PTransportChannel::SwitchSelectedConnection(Connection* conn,
                                                   IceControllerEvent reason) {
  RTC_DCHECK_RUN_ON(network_thread_);
  // Note: if conn is NULL, the previous |selected_connection_| has been
  // destroyed, so don't use it.
  Connection* old_selected_connection = selected_connection_;
  selected_connection_ = conn;
  LogCandidatePairConfig(conn, webrtc::IceCandidatePairConfigType::kSelected);
  network_route_.reset();
  if (old_selected_connection) {
    old_selected_connection->set_selected(false);
  }
  if (selected_connection_) {
    ++nomination_;
    selected_connection_->set_selected(true);
    if (old_selected_connection) {
      RTC_LOG(LS_INFO) << ToString() << ": Previous selected connection: "
                       << old_selected_connection->ToString();
    }
    RTC_LOG(LS_INFO) << ToString() << ": New selected connection: "
                     << selected_connection_->ToString();
    SignalRouteChange(this, selected_connection_->remote_candidate());
    // This is a temporary, but safe fix to webrtc issue 5705.
    // TODO(honghaiz): Make all ENOTCONN error routed through the transport
    // channel so that it knows whether the media channel is allowed to
    // send; then it will only signal ready-to-send if the media channel
    // has been disallowed to send.
    if (selected_connection_->writable() ||
        PresumedWritable(selected_connection_)) {
      SignalReadyToSend(this);
    }

    network_route_.emplace(rtc::NetworkRoute());
    network_route_->connected = ReadyToSend(selected_connection_);
    network_route_->local_network_id =
        selected_connection_->local_candidate().network_id();
    network_route_->remote_network_id =
        selected_connection_->remote_candidate().network_id();
    network_route_->last_sent_packet_id = last_sent_packet_id_;
    network_route_->packet_overhead =
        GetIpOverhead(
            selected_connection_->local_candidate().address().family()) +
        GetProtocolOverhead(selected_connection_->local_candidate().protocol());
  } else {
    RTC_LOG(LS_INFO) << ToString() << ": No selected connection";
  }
  SignalNetworkRouteChanged(network_route_);

  // Create event for candidate pair change.
  if (selected_connection_) {
    CandidatePairChangeEvent pair_change;
    pair_change.reason = reason.ToString();
    pair_change.selected_candidate_pair = *GetSelectedCandidatePair();
    pair_change.last_data_received_ms =
        selected_connection_->last_data_received();
    SignalCandidatePairChanged(pair_change);
  }

  ++selected_candidate_pair_changes_;

  ice_controller_->SetSelectedConnection(selected_connection_);
}

// Warning: UpdateState should eventually be called whenever a connection
// is added, deleted, or the write state of any connection changes so that the
// transport controller will get the up-to-date channel state. However it
// should not be called too often; in the case that multiple connection states
// change, it should be called after all the connection states have changed. For
// example, we call this at the end of SortConnectionsAndUpdateState.
void P2PTransportChannel::UpdateState() {
  RTC_DCHECK_RUN_ON(network_thread_);
  // If our selected connection is "presumed writable" (TURN-TURN with no
  // CreatePermission required), act like we're already writable to the upper
  // layers, so they can start media quicker.
  bool writable =
      selected_connection_ && (selected_connection_->writable() ||
                               PresumedWritable(selected_connection_));
  SetWritable(writable);

  bool receiving = false;
  for (const Connection* connection : connections()) {
    if (connection->receiving()) {
      receiving = true;
      break;
    }
  }
  SetReceiving(receiving);

  IceTransportState state = ComputeState();
  webrtc::IceTransportState current_standardized_state =
      ComputeIceTransportState();

  if (state_ != state) {
    RTC_LOG(LS_INFO) << ToString() << ": Transport channel state changed from "
                     << static_cast<int>(state_) << " to "
                     << static_cast<int>(state);
    // Check that the requested transition is allowed. Note that
    // P2PTransportChannel does not (yet) implement a direct mapping of the ICE
    // states from the standard; the difference is covered by
    // TransportController and PeerConnection.
    switch (state_) {
      case IceTransportState::STATE_INIT:
        // TODO(deadbeef): Once we implement end-of-candidates signaling,
        // we shouldn't go from INIT to COMPLETED.
        RTC_DCHECK(state == IceTransportState::STATE_CONNECTING ||
                   state == IceTransportState::STATE_COMPLETED);
        break;
      case IceTransportState::STATE_CONNECTING:
        RTC_DCHECK(state == IceTransportState::STATE_COMPLETED ||
                   state == IceTransportState::STATE_FAILED);
        break;
      case IceTransportState::STATE_COMPLETED:
        // TODO(deadbeef): Once we implement end-of-candidates signaling,
        // we shouldn't go from COMPLETED to CONNECTING.
        // Though we *can* go from COMPlETED to FAILED, if consent expires.
        RTC_DCHECK(state == IceTransportState::STATE_CONNECTING ||
                   state == IceTransportState::STATE_FAILED);
        break;
      case IceTransportState::STATE_FAILED:
        // TODO(deadbeef): Once we implement end-of-candidates signaling,
        // we shouldn't go from FAILED to CONNECTING or COMPLETED.
        RTC_DCHECK(state == IceTransportState::STATE_CONNECTING ||
                   state == IceTransportState::STATE_COMPLETED);
        break;
      default:
        RTC_NOTREACHED();
        break;
    }
    state_ = state;
    SignalStateChanged(this);
  }

  if (standardized_state_ != current_standardized_state) {
    standardized_state_ = current_standardized_state;
    SignalIceTransportStateChanged(this);
  }
}

void P2PTransportChannel::MaybeStopPortAllocatorSessions() {
  RTC_DCHECK_RUN_ON(network_thread_);
  if (!IsGettingPorts()) {
    return;
  }

  for (const auto& session : allocator_sessions_) {
    if (session->IsStopped()) {
      continue;
    }
    // If gathering continually, keep the last session running so that
    // it can gather candidates if the networks change.
    if (config_.gather_continually() && session == allocator_sessions_.back()) {
      session->ClearGettingPorts();
    } else {
      session->StopGettingPorts();
    }
  }
}

// If all connections timed out, delete them all.
void P2PTransportChannel::HandleAllTimedOut() {
  RTC_DCHECK_RUN_ON(network_thread_);
  for (Connection* connection : connections()) {
    connection->Destroy();
  }
}

bool P2PTransportChannel::ReadyToSend(Connection* connection) const {
  RTC_DCHECK_RUN_ON(network_thread_);
  // Note that we allow sending on an unreliable connection, because it's
  // possible that it became unreliable simply due to bad chance.
  // So this shouldn't prevent attempting to send media.
  return connection != nullptr &&
         (connection->writable() ||
          connection->write_state() == Connection::STATE_WRITE_UNRELIABLE ||
          PresumedWritable(connection));
}

// Handle queued up check-and-ping request
void P2PTransportChannel::CheckAndPing() {
  RTC_DCHECK_RUN_ON(network_thread_);
  // Make sure the states of the connections are up-to-date (since this affects
  // which ones are pingable).
  UpdateConnectionStates();

  auto result = ice_controller_->SelectConnectionToPing(last_ping_sent_ms_);
  Connection* conn = result.first;
  int delay = result.second;

  if (conn) {
    PingConnection(conn);
    MarkConnectionPinged(conn);
  }

  invoker_.AsyncInvokeDelayed<void>(
      RTC_FROM_HERE, thread(),
      rtc::Bind(&P2PTransportChannel::CheckAndPing, this), delay);
}

// This method is only for unit testing.
Connection* P2PTransportChannel::FindNextPingableConnection() {
  RTC_DCHECK_RUN_ON(network_thread_);
  return const_cast<Connection*>(ice_controller_->FindNextPingableConnection());
}

// A connection is considered a backup connection if the channel state
// is completed, the connection is not the selected connection and it is active.
void P2PTransportChannel::MarkConnectionPinged(Connection* conn) {
  RTC_DCHECK_RUN_ON(network_thread_);
  ice_controller_->MarkConnectionPinged(conn);
}

// Apart from sending ping from |conn| this method also updates
// |use_candidate_attr| and |nomination| flags. One of the flags is set to
// nominate |conn| if this channel is in CONTROLLING.
void P2PTransportChannel::PingConnection(Connection* conn) {
  RTC_DCHECK_RUN_ON(network_thread_);
  bool use_candidate_attr = false;
  uint32_t nomination = 0;
  if (ice_role_ == ICEROLE_CONTROLLING) {
    bool renomination_supported = ice_parameters_.renomination &&
                                  !remote_ice_parameters_.empty() &&
                                  remote_ice_parameters_.back().renomination;
    if (renomination_supported) {
      nomination = GetNominationAttr(conn);
    } else {
      use_candidate_attr = GetUseCandidateAttr(conn);
    }
  }
  conn->set_nomination(nomination);
  conn->set_use_candidate_attr(use_candidate_attr);
  last_ping_sent_ms_ = rtc::TimeMillis();
  conn->Ping(last_ping_sent_ms_);
}

uint32_t P2PTransportChannel::GetNominationAttr(Connection* conn) const {
  RTC_DCHECK_RUN_ON(network_thread_);
  return (conn == selected_connection_) ? nomination_ : 0;
}

// Nominate a connection based on the NominationMode.
bool P2PTransportChannel::GetUseCandidateAttr(Connection* conn) const {
  RTC_DCHECK_RUN_ON(network_thread_);
  return ice_controller_->GetUseCandidateAttr(
      conn, config_.default_nomination_mode, remote_ice_mode_);
}

// When a connection's state changes, we need to figure out who to use as
// the selected connection again.  It could have become usable, or become
// unusable.
void P2PTransportChannel::OnConnectionStateChange(Connection* connection) {
  RTC_DCHECK_RUN_ON(network_thread_);

  // May stop the allocator session when at least one connection becomes
  // strongly connected after starting to get ports and the local candidate of
  // the connection is at the latest generation. It is not enough to check
  // that the connection becomes weakly connected because the connection may be
  // changing from (writable, receiving) to (writable, not receiving).
  bool strongly_connected = !connection->weak();
  bool latest_generation = connection->local_candidate().generation() >=
                           allocator_session()->generation();
  if (strongly_connected && latest_generation) {
    MaybeStopPortAllocatorSessions();
  }
  // We have to unroll the stack before doing this because we may be changing
  // the state of connections while sorting.
  RequestSortAndStateUpdate(
      IceControllerEvent::CONNECT_STATE_CHANGE);  // "candidate pair state
                                                  // changed");
}

// When a connection is removed, edit it out, and then update our best
// connection.
void P2PTransportChannel::OnConnectionDestroyed(Connection* connection) {
  RTC_DCHECK_RUN_ON(network_thread_);

  // Note: the previous selected_connection_ may be destroyed by now, so don't
  // use it.

  // Remove this connection from the list.
  ice_controller_->OnConnectionDestroyed(connection);

  RTC_LOG(LS_INFO) << ToString() << ": Removed connection " << connection
                   << " (" << connections().size() << " remaining)";

  // If this is currently the selected connection, then we need to pick a new
  // one. The call to SortConnectionsAndUpdateState will pick a new one. It
  // looks at the current selected connection in order to avoid switching
  // between fairly similar ones. Since this connection is no longer an option,
  // we can just set selected to nullptr and re-choose a best assuming that
  // there was no selected connection.
  if (selected_connection_ == connection) {
    RTC_LOG(LS_INFO) << "Selected connection destroyed. Will choose a new one.";
    IceControllerEvent reason =
        IceControllerEvent::SELECTED_CONNECTION_DESTROYED;
    SwitchSelectedConnection(nullptr, reason);
    RequestSortAndStateUpdate(reason);
  } else {
    // If a non-selected connection was destroyed, we don't need to re-sort but
    // we do need to update state, because we could be switching to "failed" or
    // "completed".
    UpdateState();
  }
}

// When a port is destroyed, remove it from our list of ports to use for
// connection attempts.
void P2PTransportChannel::OnPortDestroyed(PortInterface* port) {
  RTC_DCHECK_RUN_ON(network_thread_);

  ports_.erase(std::remove(ports_.begin(), ports_.end(), port), ports_.end());
  pruned_ports_.erase(
      std::remove(pruned_ports_.begin(), pruned_ports_.end(), port),
      pruned_ports_.end());
  RTC_LOG(INFO) << "Removed port because it is destroyed: " << ports_.size()
                << " remaining";
}

void P2PTransportChannel::OnPortsPruned(
    PortAllocatorSession* session,
    const std::vector<PortInterface*>& ports) {
  RTC_DCHECK_RUN_ON(network_thread_);
  for (PortInterface* port : ports) {
    if (PrunePort(port)) {
      RTC_LOG(INFO) << "Removed port: " << port->ToString() << " "
                    << ports_.size() << " remaining";
    }
  }
}

void P2PTransportChannel::OnCandidatesRemoved(
    PortAllocatorSession* session,
    const std::vector<Candidate>& candidates) {
  RTC_DCHECK_RUN_ON(network_thread_);
  // Do not signal candidate removals if continual gathering is not enabled, or
  // if this is not the last session because an ICE restart would have signaled
  // the remote side to remove all candidates in previous sessions.
  if (!config_.gather_continually() || session != allocator_session()) {
    return;
  }

  std::vector<Candidate> candidates_to_remove;
  for (Candidate candidate : candidates) {
    candidate.set_transport_name(transport_name());
    candidates_to_remove.push_back(candidate);
  }
  SignalCandidatesRemoved(this, candidates_to_remove);
}

void P2PTransportChannel::PruneAllPorts() {
  RTC_DCHECK_RUN_ON(network_thread_);
  pruned_ports_.insert(pruned_ports_.end(), ports_.begin(), ports_.end());
  ports_.clear();
}

bool P2PTransportChannel::PrunePort(PortInterface* port) {
  RTC_DCHECK_RUN_ON(network_thread_);
  auto it = absl::c_find(ports_, port);
  // Don't need to do anything if the port has been deleted from the port list.
  if (it == ports_.end()) {
    return false;
  }
  ports_.erase(it);
  pruned_ports_.push_back(port);
  return true;
}

// We data is available, let listeners know
void P2PTransportChannel::OnReadPacket(Connection* connection,
                                       const char* data,
                                       size_t len,
                                       int64_t packet_time_us) {
  RTC_DCHECK_RUN_ON(network_thread_);

  if (connection == selected_connection_) {
    // Let the client know of an incoming packet
    SignalReadPacket(this, data, len, packet_time_us, 0);
    return;
  }

  // Do not deliver, if packet doesn't belong to the correct transport channel.
  if (!FindConnection(connection))
    return;

  // Let the client know of an incoming packet
  SignalReadPacket(this, data, len, packet_time_us, 0);

  // May need to switch the sending connection based on the receiving media path
  // if this is the controlled side.
  if (ice_role_ == ICEROLE_CONTROLLED) {
    MaybeSwitchSelectedConnection(connection,
                                  IceControllerEvent::DATA_RECEIVED);
  }
}

void P2PTransportChannel::OnSentPacket(const rtc::SentPacket& sent_packet) {
  RTC_DCHECK_RUN_ON(network_thread_);

  SignalSentPacket(this, sent_packet);
}

void P2PTransportChannel::OnReadyToSend(Connection* connection) {
  RTC_DCHECK_RUN_ON(network_thread_);
  if (connection == selected_connection_ && writable()) {
    SignalReadyToSend(this);
  }
}

void P2PTransportChannel::SetWritable(bool writable) {
  RTC_DCHECK_RUN_ON(network_thread_);
  if (writable_ == writable) {
    return;
  }
  RTC_LOG(LS_VERBOSE) << ToString() << ": Changed writable_ to " << writable;
  writable_ = writable;
  if (writable_) {
    has_been_writable_ = true;
    SignalReadyToSend(this);
  }
  SignalWritableState(this);
}

void P2PTransportChannel::SetReceiving(bool receiving) {
  RTC_DCHECK_RUN_ON(network_thread_);
  if (receiving_ == receiving) {
    return;
  }
  receiving_ = receiving;
  SignalReceivingState(this);
}

Candidate P2PTransportChannel::SanitizeLocalCandidate(
    const Candidate& c) const {
  RTC_DCHECK_RUN_ON(network_thread_);
  // Delegates to the port allocator.
  return allocator_->SanitizeCandidate(c);
}

Candidate P2PTransportChannel::SanitizeRemoteCandidate(
    const Candidate& c) const {
  RTC_DCHECK_RUN_ON(network_thread_);
  // If the remote endpoint signaled us an mDNS candidate, we assume it
  // is supposed to be sanitized.
  bool use_hostname_address = absl::EndsWith(c.address().hostname(), LOCAL_TLD);
  // Remove the address for prflx remote candidates. See
  // https://w3c.github.io/webrtc-stats/#dom-rtcicecandidatestats.
  use_hostname_address |= c.type() == PRFLX_PORT_TYPE;
  return c.ToSanitizedCopy(use_hostname_address,
                           false /* filter_related_address */);
}

void P2PTransportChannel::LogCandidatePairConfig(
    Connection* conn,
    webrtc::IceCandidatePairConfigType type) {
  RTC_DCHECK_RUN_ON(network_thread_);
  if (conn == nullptr) {
    return;
  }
  ice_event_log_.LogCandidatePairConfig(type, conn->id(),
                                        conn->ToLogDescription());
}

}  // namespace cricket
