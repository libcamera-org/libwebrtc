/*
 *  Copyright (c) 2021 The WebRTC project authors. All Rights Reserved.
 *
 *  Use of this source code is governed by a BSD-style license
 *  that can be found in the LICENSE file in the root of the source
 *  tree. An additional intellectual property rights grant can be found
 *  in the file PATENTS.  All contributing project authors may
 *  be found in the AUTHORS file in the root of the source tree.
 */
#include "net/dcsctp/socket/dcsctp_socket.h"

#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "api/array_view.h"
#include "net/dcsctp/common/handover_testing.h"
#include "net/dcsctp/packet/chunk/chunk.h"
#include "net/dcsctp/packet/chunk/cookie_echo_chunk.h"
#include "net/dcsctp/packet/chunk/data_chunk.h"
#include "net/dcsctp/packet/chunk/data_common.h"
#include "net/dcsctp/packet/chunk/error_chunk.h"
#include "net/dcsctp/packet/chunk/heartbeat_ack_chunk.h"
#include "net/dcsctp/packet/chunk/heartbeat_request_chunk.h"
#include "net/dcsctp/packet/chunk/idata_chunk.h"
#include "net/dcsctp/packet/chunk/init_chunk.h"
#include "net/dcsctp/packet/chunk/sack_chunk.h"
#include "net/dcsctp/packet/chunk/shutdown_chunk.h"
#include "net/dcsctp/packet/error_cause/error_cause.h"
#include "net/dcsctp/packet/error_cause/unrecognized_chunk_type_cause.h"
#include "net/dcsctp/packet/parameter/heartbeat_info_parameter.h"
#include "net/dcsctp/packet/parameter/parameter.h"
#include "net/dcsctp/packet/sctp_packet.h"
#include "net/dcsctp/packet/tlv_trait.h"
#include "net/dcsctp/public/dcsctp_message.h"
#include "net/dcsctp/public/dcsctp_options.h"
#include "net/dcsctp/public/dcsctp_socket.h"
#include "net/dcsctp/public/text_pcap_packet_observer.h"
#include "net/dcsctp/public/types.h"
#include "net/dcsctp/rx/reassembly_queue.h"
#include "net/dcsctp/socket/mock_dcsctp_socket_callbacks.h"
#include "net/dcsctp/testing/testing_macros.h"
#include "rtc_base/gunit.h"
#include "test/gmock.h"

ABSL_FLAG(bool, dcsctp_capture_packets, false, "Print packet capture.");

namespace dcsctp {
namespace {
using ::testing::_;
using ::testing::AllOf;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAre;

constexpr SendOptions kSendOptions;
constexpr size_t kLargeMessageSize = DcSctpOptions::kMaxSafeMTUSize * 20;
constexpr size_t kSmallMessageSize = 10;
constexpr int kMaxBurstPackets = 4;

MATCHER_P(HasDataChunkWithStreamId, stream_id, "") {
  absl::optional<SctpPacket> packet = SctpPacket::Parse(arg);
  if (!packet.has_value()) {
    *result_listener << "data didn't parse as an SctpPacket";
    return false;
  }

  if (packet->descriptors()[0].type != DataChunk::kType) {
    *result_listener << "the first chunk in the packet is not a data chunk";
    return false;
  }

  absl::optional<DataChunk> dc =
      DataChunk::Parse(packet->descriptors()[0].data);
  if (!dc.has_value()) {
    *result_listener << "The first chunk didn't parse as a data chunk";
    return false;
  }

  if (dc->stream_id() != stream_id) {
    *result_listener << "the stream_id is " << *dc->stream_id();
    return false;
  }

  return true;
}

MATCHER_P(HasDataChunkWithPPID, ppid, "") {
  absl::optional<SctpPacket> packet = SctpPacket::Parse(arg);
  if (!packet.has_value()) {
    *result_listener << "data didn't parse as an SctpPacket";
    return false;
  }

  if (packet->descriptors()[0].type != DataChunk::kType) {
    *result_listener << "the first chunk in the packet is not a data chunk";
    return false;
  }

  absl::optional<DataChunk> dc =
      DataChunk::Parse(packet->descriptors()[0].data);
  if (!dc.has_value()) {
    *result_listener << "The first chunk didn't parse as a data chunk";
    return false;
  }

  if (dc->ppid() != ppid) {
    *result_listener << "the ppid is " << *dc->ppid();
    return false;
  }

  return true;
}

MATCHER_P(HasDataChunkWithSsn, ssn, "") {
  absl::optional<SctpPacket> packet = SctpPacket::Parse(arg);
  if (!packet.has_value()) {
    *result_listener << "data didn't parse as an SctpPacket";
    return false;
  }

  if (packet->descriptors()[0].type != DataChunk::kType) {
    *result_listener << "the first chunk in the packet is not a data chunk";
    return false;
  }

  absl::optional<DataChunk> dc =
      DataChunk::Parse(packet->descriptors()[0].data);
  if (!dc.has_value()) {
    *result_listener << "The first chunk didn't parse as a data chunk";
    return false;
  }

  if (dc->ssn() != ssn) {
    *result_listener << "the ssn is " << *dc->ssn();
    return false;
  }

  return true;
}

MATCHER_P(HasDataChunkWithMid, mid, "") {
  absl::optional<SctpPacket> packet = SctpPacket::Parse(arg);
  if (!packet.has_value()) {
    *result_listener << "data didn't parse as an SctpPacket";
    return false;
  }

  if (packet->descriptors()[0].type != IDataChunk::kType) {
    *result_listener << "the first chunk in the packet is not an i-data chunk";
    return false;
  }

  absl::optional<IDataChunk> dc =
      IDataChunk::Parse(packet->descriptors()[0].data);
  if (!dc.has_value()) {
    *result_listener << "The first chunk didn't parse as an i-data chunk";
    return false;
  }

  if (dc->message_id() != mid) {
    *result_listener << "the mid is " << *dc->message_id();
    return false;
  }

  return true;
}

MATCHER_P(HasSackWithCumAckTsn, tsn, "") {
  absl::optional<SctpPacket> packet = SctpPacket::Parse(arg);
  if (!packet.has_value()) {
    *result_listener << "data didn't parse as an SctpPacket";
    return false;
  }

  if (packet->descriptors()[0].type != SackChunk::kType) {
    *result_listener << "the first chunk in the packet is not a data chunk";
    return false;
  }

  absl::optional<SackChunk> sc =
      SackChunk::Parse(packet->descriptors()[0].data);
  if (!sc.has_value()) {
    *result_listener << "The first chunk didn't parse as a data chunk";
    return false;
  }

  if (sc->cumulative_tsn_ack() != tsn) {
    *result_listener << "the cum_ack_tsn is " << *sc->cumulative_tsn_ack();
    return false;
  }

  return true;
}

MATCHER(HasSackWithNoGapAckBlocks, "") {
  absl::optional<SctpPacket> packet = SctpPacket::Parse(arg);
  if (!packet.has_value()) {
    *result_listener << "data didn't parse as an SctpPacket";
    return false;
  }

  if (packet->descriptors()[0].type != SackChunk::kType) {
    *result_listener << "the first chunk in the packet is not a data chunk";
    return false;
  }

  absl::optional<SackChunk> sc =
      SackChunk::Parse(packet->descriptors()[0].data);
  if (!sc.has_value()) {
    *result_listener << "The first chunk didn't parse as a data chunk";
    return false;
  }

  if (!sc->gap_ack_blocks().empty()) {
    *result_listener << "there are gap ack blocks";
    return false;
  }

  return true;
}

TSN AddTo(TSN tsn, int delta) {
  return TSN(*tsn + delta);
}

DcSctpOptions FixupOptions(DcSctpOptions options = {}) {
  DcSctpOptions fixup = options;
  // To make the interval more predictable in tests.
  fixup.heartbeat_interval_include_rtt = false;
  fixup.max_burst = kMaxBurstPackets;
  return fixup;
}

std::unique_ptr<PacketObserver> GetPacketObserver(absl::string_view name) {
  if (absl::GetFlag(FLAGS_dcsctp_capture_packets)) {
    return std::make_unique<TextPcapPacketObserver>(name);
  }
  return nullptr;
}

struct SocketUnderTest {
  explicit SocketUnderTest(absl::string_view name,
                           const DcSctpOptions& opts = {})
      : options(FixupOptions(opts)),
        cb(name),
        socket(name, cb, GetPacketObserver(name), options) {}

  const DcSctpOptions options;
  testing::NiceMock<MockDcSctpSocketCallbacks> cb;
  DcSctpSocket socket;
};

void ExchangeMessages(SocketUnderTest& a, SocketUnderTest& z) {
  bool delivered_packet = false;
  do {
    delivered_packet = false;
    std::vector<uint8_t> packet_from_a = a.cb.ConsumeSentPacket();
    if (!packet_from_a.empty()) {
      delivered_packet = true;
      z.socket.ReceivePacket(std::move(packet_from_a));
    }
    std::vector<uint8_t> packet_from_z = z.cb.ConsumeSentPacket();
    if (!packet_from_z.empty()) {
      delivered_packet = true;
      a.socket.ReceivePacket(std::move(packet_from_z));
    }
  } while (delivered_packet);
}

void RunTimers(SocketUnderTest& s) {
  for (;;) {
    absl::optional<TimeoutID> timeout_id = s.cb.GetNextExpiredTimeout();
    if (!timeout_id.has_value()) {
      break;
    }
    s.socket.HandleTimeout(*timeout_id);
  }
}

void AdvanceTime(SocketUnderTest& a, SocketUnderTest& z, DurationMs duration) {
  a.cb.AdvanceTime(duration);
  z.cb.AdvanceTime(duration);

  RunTimers(a);
  RunTimers(z);
}

// Calls Connect() on `sock_a_` and make the connection established.
void ConnectSockets(SocketUnderTest& a, SocketUnderTest& z) {
  EXPECT_CALL(a.cb, OnConnected).Times(1);
  EXPECT_CALL(z.cb, OnConnected).Times(1);

  a.socket.Connect();
  // Z reads INIT, INIT_ACK, COOKIE_ECHO, COOKIE_ACK
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());
  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());
  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());

  EXPECT_EQ(a.socket.state(), SocketState::kConnected);
  EXPECT_EQ(z.socket.state(), SocketState::kConnected);
}

std::unique_ptr<SocketUnderTest> HandoverSocket(
    std::unique_ptr<SocketUnderTest> sut) {
  EXPECT_EQ(sut->socket.GetHandoverReadiness(), HandoverReadinessStatus());

  bool is_closed = sut->socket.state() == SocketState::kClosed;
  if (!is_closed) {
    EXPECT_CALL(sut->cb, OnClosed).Times(1);
  }
  absl::optional<DcSctpSocketHandoverState> handover_state =
      sut->socket.GetHandoverStateAndClose();
  EXPECT_TRUE(handover_state.has_value());
  g_handover_state_transformer_for_test(&*handover_state);

  auto handover_socket = std::make_unique<SocketUnderTest>("H", sut->options);
  if (!is_closed) {
    EXPECT_CALL(handover_socket->cb, OnConnected).Times(1);
  }
  handover_socket->socket.RestoreFromState(*handover_state);
  return handover_socket;
}

// Test parameter that controls whether to perform handovers during the test. A
// test can have multiple points where it conditionally hands over socket Z.
// Either socket Z will be handed over at all those points or handed over never.
enum class HandoverMode {
  kNoHandover,
  kPerformHandovers,
};

class DcSctpSocketParametrizedTest
    : public ::testing::Test,
      public ::testing::WithParamInterface<HandoverMode> {
 protected:
  // Trigger handover for `sut` depending on the current test param.
  std::unique_ptr<SocketUnderTest> MaybeHandoverSocket(
      std::unique_ptr<SocketUnderTest> sut) {
    if (GetParam() == HandoverMode::kPerformHandovers) {
      return HandoverSocket(std::move(sut));
    }
    return sut;
  }

  // Trigger handover for socket Z depending on the current test param.
  // Then checks message passing to verify the handed over socket is functional.
  void MaybeHandoverSocketAndSendMessage(SocketUnderTest& a,
                                         std::unique_ptr<SocketUnderTest> z) {
    if (GetParam() == HandoverMode::kPerformHandovers) {
      z = HandoverSocket(std::move(z));
    }

    ExchangeMessages(a, *z);
    a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), {1, 2}), kSendOptions);
    ExchangeMessages(a, *z);

    absl::optional<DcSctpMessage> msg = z->cb.ConsumeReceivedMessage();
    ASSERT_TRUE(msg.has_value());
    EXPECT_EQ(msg->stream_id(), StreamID(1));
  }
};

INSTANTIATE_TEST_SUITE_P(Handovers,
                         DcSctpSocketParametrizedTest,
                         testing::Values(HandoverMode::kNoHandover,
                                         HandoverMode::kPerformHandovers),
                         [](const auto& test_info) {
                           return test_info.param ==
                                          HandoverMode::kPerformHandovers
                                      ? "WithHandovers"
                                      : "NoHandover";
                         });

TEST(DcSctpSocketTest, EstablishConnection) {
  SocketUnderTest a("A");
  SocketUnderTest z("Z");

  EXPECT_CALL(a.cb, OnConnected).Times(1);
  EXPECT_CALL(z.cb, OnConnected).Times(1);
  EXPECT_CALL(a.cb, OnConnectionRestarted).Times(0);
  EXPECT_CALL(z.cb, OnConnectionRestarted).Times(0);

  a.socket.Connect();
  // Z reads INIT, produces INIT_ACK
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());
  // A reads INIT_ACK, produces COOKIE_ECHO
  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());
  // Z reads COOKIE_ECHO, produces COOKIE_ACK
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());
  // A reads COOKIE_ACK.
  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());

  EXPECT_EQ(a.socket.state(), SocketState::kConnected);
  EXPECT_EQ(z.socket.state(), SocketState::kConnected);
}

TEST(DcSctpSocketTest, EstablishConnectionWithSetupCollision) {
  SocketUnderTest a("A");
  SocketUnderTest z("Z");

  EXPECT_CALL(a.cb, OnConnected).Times(1);
  EXPECT_CALL(z.cb, OnConnected).Times(1);
  EXPECT_CALL(a.cb, OnConnectionRestarted).Times(0);
  EXPECT_CALL(z.cb, OnConnectionRestarted).Times(0);
  a.socket.Connect();
  z.socket.Connect();

  ExchangeMessages(a, z);

  EXPECT_EQ(a.socket.state(), SocketState::kConnected);
  EXPECT_EQ(z.socket.state(), SocketState::kConnected);
}

TEST(DcSctpSocketTest, ShuttingDownWhileEstablishingConnection) {
  SocketUnderTest a("A");
  SocketUnderTest z("Z");

  EXPECT_CALL(a.cb, OnConnected).Times(0);
  EXPECT_CALL(z.cb, OnConnected).Times(1);
  a.socket.Connect();

  // Z reads INIT, produces INIT_ACK
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());
  // A reads INIT_ACK, produces COOKIE_ECHO
  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());
  // Z reads COOKIE_ECHO, produces COOKIE_ACK
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());
  // Drop COOKIE_ACK, just to more easily verify shutdown protocol.
  z.cb.ConsumeSentPacket();

  // As Socket A has received INIT_ACK, it has a TCB and is connected, while
  // Socket Z needs to receive COOKIE_ECHO to get there. Socket A still has
  // timers running at this point.
  EXPECT_EQ(a.socket.state(), SocketState::kConnecting);
  EXPECT_EQ(z.socket.state(), SocketState::kConnected);

  // Socket A is now shut down, which should make it stop those timers.
  a.socket.Shutdown();

  EXPECT_CALL(a.cb, OnClosed).Times(1);
  EXPECT_CALL(z.cb, OnClosed).Times(1);

  // Z reads SHUTDOWN, produces SHUTDOWN_ACK
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());
  // A reads SHUTDOWN_ACK, produces SHUTDOWN_COMPLETE
  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());
  // Z reads SHUTDOWN_COMPLETE.
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());

  EXPECT_TRUE(a.cb.ConsumeSentPacket().empty());
  EXPECT_TRUE(z.cb.ConsumeSentPacket().empty());

  EXPECT_EQ(a.socket.state(), SocketState::kClosed);
  EXPECT_EQ(z.socket.state(), SocketState::kClosed);
}

TEST(DcSctpSocketTest, EstablishSimultaneousConnection) {
  SocketUnderTest a("A");
  SocketUnderTest z("Z");

  EXPECT_CALL(a.cb, OnConnected).Times(1);
  EXPECT_CALL(z.cb, OnConnected).Times(1);
  EXPECT_CALL(a.cb, OnConnectionRestarted).Times(0);
  EXPECT_CALL(z.cb, OnConnectionRestarted).Times(0);
  a.socket.Connect();

  // INIT isn't received by Z, as it wasn't ready yet.
  a.cb.ConsumeSentPacket();

  z.socket.Connect();

  // A reads INIT, produces INIT_ACK
  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());

  // Z reads INIT_ACK, sends COOKIE_ECHO
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());

  // A reads COOKIE_ECHO - establishes connection.
  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());

  EXPECT_EQ(a.socket.state(), SocketState::kConnected);

  // Proceed with the remaining packets.
  ExchangeMessages(a, z);

  EXPECT_EQ(a.socket.state(), SocketState::kConnected);
  EXPECT_EQ(z.socket.state(), SocketState::kConnected);
}

TEST(DcSctpSocketTest, EstablishConnectionLostCookieAck) {
  SocketUnderTest a("A");
  SocketUnderTest z("Z");

  EXPECT_CALL(a.cb, OnConnected).Times(1);
  EXPECT_CALL(z.cb, OnConnected).Times(1);
  EXPECT_CALL(a.cb, OnConnectionRestarted).Times(0);
  EXPECT_CALL(z.cb, OnConnectionRestarted).Times(0);

  a.socket.Connect();
  // Z reads INIT, produces INIT_ACK
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());
  // A reads INIT_ACK, produces COOKIE_ECHO
  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());
  // Z reads COOKIE_ECHO, produces COOKIE_ACK
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());
  // COOKIE_ACK is lost.
  z.cb.ConsumeSentPacket();

  EXPECT_EQ(a.socket.state(), SocketState::kConnecting);
  EXPECT_EQ(z.socket.state(), SocketState::kConnected);

  // This will make A re-send the COOKIE_ECHO
  AdvanceTime(a, z, DurationMs(a.options.t1_cookie_timeout));

  // Z reads COOKIE_ECHO, produces COOKIE_ACK
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());
  // A reads COOKIE_ACK.
  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());

  EXPECT_EQ(a.socket.state(), SocketState::kConnected);
  EXPECT_EQ(z.socket.state(), SocketState::kConnected);
}

TEST(DcSctpSocketTest, ResendInitAndEstablishConnection) {
  SocketUnderTest a("A");
  SocketUnderTest z("Z");

  a.socket.Connect();
  // INIT is never received by Z.
  ASSERT_HAS_VALUE_AND_ASSIGN(SctpPacket init_packet,
                              SctpPacket::Parse(a.cb.ConsumeSentPacket()));
  EXPECT_EQ(init_packet.descriptors()[0].type, InitChunk::kType);

  AdvanceTime(a, z, a.options.t1_init_timeout);

  // Z reads INIT, produces INIT_ACK
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());
  // A reads INIT_ACK, produces COOKIE_ECHO
  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());
  // Z reads COOKIE_ECHO, produces COOKIE_ACK
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());
  // A reads COOKIE_ACK.
  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());

  EXPECT_EQ(a.socket.state(), SocketState::kConnected);
  EXPECT_EQ(z.socket.state(), SocketState::kConnected);
}

TEST(DcSctpSocketTest, ResendingInitTooManyTimesAborts) {
  SocketUnderTest a("A");
  SocketUnderTest z("Z");

  a.socket.Connect();

  // INIT is never received by Z.
  ASSERT_HAS_VALUE_AND_ASSIGN(SctpPacket init_packet,
                              SctpPacket::Parse(a.cb.ConsumeSentPacket()));
  EXPECT_EQ(init_packet.descriptors()[0].type, InitChunk::kType);

  for (int i = 0; i < *a.options.max_init_retransmits; ++i) {
    AdvanceTime(a, z, a.options.t1_init_timeout * (1 << i));

    // INIT is resent
    ASSERT_HAS_VALUE_AND_ASSIGN(SctpPacket resent_init_packet,
                                SctpPacket::Parse(a.cb.ConsumeSentPacket()));
    EXPECT_EQ(resent_init_packet.descriptors()[0].type, InitChunk::kType);
  }

  // Another timeout, after the max init retransmits.
  EXPECT_CALL(a.cb, OnAborted).Times(1);
  AdvanceTime(
      a, z, a.options.t1_init_timeout * (1 << *a.options.max_init_retransmits));

  EXPECT_EQ(a.socket.state(), SocketState::kClosed);
}

TEST(DcSctpSocketTest, ResendCookieEchoAndEstablishConnection) {
  SocketUnderTest a("A");
  SocketUnderTest z("Z");

  a.socket.Connect();

  // Z reads INIT, produces INIT_ACK
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());
  // A reads INIT_ACK, produces COOKIE_ECHO
  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());

  // COOKIE_ECHO is never received by Z.
  ASSERT_HAS_VALUE_AND_ASSIGN(SctpPacket init_packet,
                              SctpPacket::Parse(a.cb.ConsumeSentPacket()));
  EXPECT_EQ(init_packet.descriptors()[0].type, CookieEchoChunk::kType);

  AdvanceTime(a, z, a.options.t1_init_timeout);

  // Z reads COOKIE_ECHO, produces COOKIE_ACK
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());
  // A reads COOKIE_ACK.
  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());

  EXPECT_EQ(a.socket.state(), SocketState::kConnected);
  EXPECT_EQ(z.socket.state(), SocketState::kConnected);
}

TEST(DcSctpSocketTest, ResendingCookieEchoTooManyTimesAborts) {
  SocketUnderTest a("A");
  SocketUnderTest z("Z");

  a.socket.Connect();

  // Z reads INIT, produces INIT_ACK
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());
  // A reads INIT_ACK, produces COOKIE_ECHO
  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());

  // COOKIE_ECHO is never received by Z.
  ASSERT_HAS_VALUE_AND_ASSIGN(SctpPacket init_packet,
                              SctpPacket::Parse(a.cb.ConsumeSentPacket()));
  EXPECT_EQ(init_packet.descriptors()[0].type, CookieEchoChunk::kType);

  for (int i = 0; i < *a.options.max_init_retransmits; ++i) {
    AdvanceTime(a, z, a.options.t1_cookie_timeout * (1 << i));

    // COOKIE_ECHO is resent
    ASSERT_HAS_VALUE_AND_ASSIGN(SctpPacket resent_init_packet,
                                SctpPacket::Parse(a.cb.ConsumeSentPacket()));
    EXPECT_EQ(resent_init_packet.descriptors()[0].type, CookieEchoChunk::kType);
  }

  // Another timeout, after the max init retransmits.
  EXPECT_CALL(a.cb, OnAborted).Times(1);
  AdvanceTime(
      a, z,
      a.options.t1_cookie_timeout * (1 << *a.options.max_init_retransmits));

  EXPECT_EQ(a.socket.state(), SocketState::kClosed);
}

TEST(DcSctpSocketTest, DoesntSendMorePacketsUntilCookieAckHasBeenReceived) {
  SocketUnderTest a("A");
  SocketUnderTest z("Z");

  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53),
                              std::vector<uint8_t>(kLargeMessageSize)),
                kSendOptions);
  a.socket.Connect();

  // Z reads INIT, produces INIT_ACK
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());
  // A reads INIT_ACK, produces COOKIE_ECHO
  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());

  // COOKIE_ECHO is never received by Z.
  ASSERT_HAS_VALUE_AND_ASSIGN(SctpPacket cookie_echo_packet1,
                              SctpPacket::Parse(a.cb.ConsumeSentPacket()));
  EXPECT_THAT(cookie_echo_packet1.descriptors(), SizeIs(2));
  EXPECT_EQ(cookie_echo_packet1.descriptors()[0].type, CookieEchoChunk::kType);
  EXPECT_EQ(cookie_echo_packet1.descriptors()[1].type, DataChunk::kType);

  EXPECT_THAT(a.cb.ConsumeSentPacket(), IsEmpty());

  // There are DATA chunks in the sent packet (that was lost), which means that
  // the T3-RTX timer is running, but as the socket is in kCookieEcho state, it
  // will be T1-COOKIE that drives retransmissions, so when the T3-RTX expires,
  // nothing should be retransmitted.
  ASSERT_TRUE(a.options.rto_initial < a.options.t1_cookie_timeout);
  AdvanceTime(a, z, a.options.rto_initial);
  EXPECT_THAT(a.cb.ConsumeSentPacket(), IsEmpty());

  // When T1-COOKIE expires, both the COOKIE-ECHO and DATA should be present.
  AdvanceTime(a, z, a.options.t1_cookie_timeout - a.options.rto_initial);

  // And this COOKIE-ECHO and DATA is also lost - never received by Z.
  ASSERT_HAS_VALUE_AND_ASSIGN(SctpPacket cookie_echo_packet2,
                              SctpPacket::Parse(a.cb.ConsumeSentPacket()));
  EXPECT_THAT(cookie_echo_packet2.descriptors(), SizeIs(2));
  EXPECT_EQ(cookie_echo_packet2.descriptors()[0].type, CookieEchoChunk::kType);
  EXPECT_EQ(cookie_echo_packet2.descriptors()[1].type, DataChunk::kType);

  EXPECT_THAT(a.cb.ConsumeSentPacket(), IsEmpty());

  // COOKIE_ECHO has exponential backoff.
  AdvanceTime(a, z, a.options.t1_cookie_timeout * 2);

  // Z reads COOKIE_ECHO, produces COOKIE_ACK
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());
  // A reads COOKIE_ACK.
  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());

  EXPECT_EQ(a.socket.state(), SocketState::kConnected);
  EXPECT_EQ(z.socket.state(), SocketState::kConnected);

  ExchangeMessages(a, z);
  EXPECT_THAT(z.cb.ConsumeReceivedMessage()->payload(),
              SizeIs(kLargeMessageSize));
}

TEST_P(DcSctpSocketParametrizedTest, ShutdownConnection) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  RTC_LOG(LS_INFO) << "Shutting down";

  EXPECT_CALL(z->cb, OnClosed).Times(1);
  a.socket.Shutdown();
  // Z reads SHUTDOWN, produces SHUTDOWN_ACK
  z->socket.ReceivePacket(a.cb.ConsumeSentPacket());
  // A reads SHUTDOWN_ACK, produces SHUTDOWN_COMPLETE
  a.socket.ReceivePacket(z->cb.ConsumeSentPacket());
  // Z reads SHUTDOWN_COMPLETE.
  z->socket.ReceivePacket(a.cb.ConsumeSentPacket());

  EXPECT_EQ(a.socket.state(), SocketState::kClosed);
  EXPECT_EQ(z->socket.state(), SocketState::kClosed);

  z = MaybeHandoverSocket(std::move(z));
  EXPECT_EQ(z->socket.state(), SocketState::kClosed);
}

TEST(DcSctpSocketTest, ShutdownTimerExpiresTooManyTimeClosesConnection) {
  SocketUnderTest a("A");
  SocketUnderTest z("Z");

  ConnectSockets(a, z);

  a.socket.Shutdown();
  // Drop first SHUTDOWN packet.
  a.cb.ConsumeSentPacket();

  EXPECT_EQ(a.socket.state(), SocketState::kShuttingDown);

  for (int i = 0; i < *a.options.max_retransmissions; ++i) {
    AdvanceTime(a, z, DurationMs(a.options.rto_initial * (1 << i)));

    // Dropping every shutdown chunk.
    ASSERT_HAS_VALUE_AND_ASSIGN(SctpPacket packet,
                                SctpPacket::Parse(a.cb.ConsumeSentPacket()));
    EXPECT_EQ(packet.descriptors()[0].type, ShutdownChunk::kType);
    EXPECT_TRUE(a.cb.ConsumeSentPacket().empty());
  }
  // The last expiry, makes it abort the connection.
  EXPECT_CALL(a.cb, OnAborted).Times(1);
  AdvanceTime(a, z,
              a.options.rto_initial * (1 << *a.options.max_retransmissions));

  EXPECT_EQ(a.socket.state(), SocketState::kClosed);
  ASSERT_HAS_VALUE_AND_ASSIGN(SctpPacket packet,
                              SctpPacket::Parse(a.cb.ConsumeSentPacket()));
  EXPECT_EQ(packet.descriptors()[0].type, AbortChunk::kType);
  EXPECT_TRUE(a.cb.ConsumeSentPacket().empty());
}

TEST(DcSctpSocketTest, EstablishConnectionWhileSendingData) {
  SocketUnderTest a("A");
  SocketUnderTest z("Z");

  a.socket.Connect();

  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), {1, 2}), kSendOptions);

  // Z reads INIT, produces INIT_ACK
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());
  // // A reads INIT_ACK, produces COOKIE_ECHO
  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());
  // // Z reads COOKIE_ECHO, produces COOKIE_ACK
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());
  // // A reads COOKIE_ACK.
  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());

  EXPECT_EQ(a.socket.state(), SocketState::kConnected);
  EXPECT_EQ(z.socket.state(), SocketState::kConnected);

  absl::optional<DcSctpMessage> msg = z.cb.ConsumeReceivedMessage();
  ASSERT_TRUE(msg.has_value());
  EXPECT_EQ(msg->stream_id(), StreamID(1));
}

TEST(DcSctpSocketTest, SendMessageAfterEstablished) {
  SocketUnderTest a("A");
  SocketUnderTest z("Z");

  ConnectSockets(a, z);

  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), {1, 2}), kSendOptions);
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());

  absl::optional<DcSctpMessage> msg = z.cb.ConsumeReceivedMessage();
  ASSERT_TRUE(msg.has_value());
  EXPECT_EQ(msg->stream_id(), StreamID(1));
}

TEST_P(DcSctpSocketParametrizedTest, TimeoutResendsPacket) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), {1, 2}), kSendOptions);
  a.cb.ConsumeSentPacket();

  RTC_LOG(LS_INFO) << "Advancing time";
  AdvanceTime(a, *z, a.options.rto_initial);

  z->socket.ReceivePacket(a.cb.ConsumeSentPacket());

  absl::optional<DcSctpMessage> msg = z->cb.ConsumeReceivedMessage();
  ASSERT_TRUE(msg.has_value());
  EXPECT_EQ(msg->stream_id(), StreamID(1));

  MaybeHandoverSocketAndSendMessage(a, std::move(z));
}

TEST_P(DcSctpSocketParametrizedTest, SendALotOfBytesMissedSecondPacket) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  std::vector<uint8_t> payload(kLargeMessageSize);
  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), payload), kSendOptions);

  // First DATA
  z->socket.ReceivePacket(a.cb.ConsumeSentPacket());
  // Second DATA (lost)
  a.cb.ConsumeSentPacket();

  // Retransmit and handle the rest
  ExchangeMessages(a, *z);

  absl::optional<DcSctpMessage> msg = z->cb.ConsumeReceivedMessage();
  ASSERT_TRUE(msg.has_value());
  EXPECT_EQ(msg->stream_id(), StreamID(1));
  EXPECT_THAT(msg->payload(), testing::ElementsAreArray(payload));

  MaybeHandoverSocketAndSendMessage(a, std::move(z));
}

TEST_P(DcSctpSocketParametrizedTest, SendingHeartbeatAnswersWithAck) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  // Inject a HEARTBEAT chunk
  SctpPacket::Builder b(a.socket.verification_tag(), DcSctpOptions());
  uint8_t info[] = {1, 2, 3, 4};
  Parameters::Builder params_builder;
  params_builder.Add(HeartbeatInfoParameter(info));
  b.Add(HeartbeatRequestChunk(params_builder.Build()));
  a.socket.ReceivePacket(b.Build());

  // HEARTBEAT_ACK is sent as a reply. Capture it.
  ASSERT_HAS_VALUE_AND_ASSIGN(SctpPacket ack_packet,
                              SctpPacket::Parse(a.cb.ConsumeSentPacket()));
  ASSERT_THAT(ack_packet.descriptors(), SizeIs(1));
  ASSERT_HAS_VALUE_AND_ASSIGN(
      HeartbeatAckChunk ack,
      HeartbeatAckChunk::Parse(ack_packet.descriptors()[0].data));
  ASSERT_HAS_VALUE_AND_ASSIGN(HeartbeatInfoParameter info_param, ack.info());
  EXPECT_THAT(info_param.info(), ElementsAre(1, 2, 3, 4));

  MaybeHandoverSocketAndSendMessage(a, std::move(z));
}

TEST_P(DcSctpSocketParametrizedTest, ExpectHeartbeatToBeSent) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  EXPECT_THAT(a.cb.ConsumeSentPacket(), IsEmpty());

  AdvanceTime(a, *z, a.options.heartbeat_interval);

  std::vector<uint8_t> hb_packet_raw = a.cb.ConsumeSentPacket();
  ASSERT_HAS_VALUE_AND_ASSIGN(SctpPacket hb_packet,
                              SctpPacket::Parse(hb_packet_raw));
  ASSERT_THAT(hb_packet.descriptors(), SizeIs(1));
  ASSERT_HAS_VALUE_AND_ASSIGN(
      HeartbeatRequestChunk hb,
      HeartbeatRequestChunk::Parse(hb_packet.descriptors()[0].data));
  ASSERT_HAS_VALUE_AND_ASSIGN(HeartbeatInfoParameter info_param, hb.info());

  // The info is a single 64-bit number.
  EXPECT_THAT(hb.info()->info(), SizeIs(8));

  // Feed it to Sock-z and expect a HEARTBEAT_ACK that will be propagated back.
  z->socket.ReceivePacket(hb_packet_raw);
  a.socket.ReceivePacket(z->cb.ConsumeSentPacket());

  MaybeHandoverSocketAndSendMessage(a, std::move(z));
}

TEST_P(DcSctpSocketParametrizedTest,
       CloseConnectionAfterTooManyLostHeartbeats) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  EXPECT_CALL(z->cb, OnClosed).Times(1);
  EXPECT_THAT(a.cb.ConsumeSentPacket(), testing::IsEmpty());
  // Force-close socket Z so that it doesn't interfere from now on.
  z->socket.Close();

  DurationMs time_to_next_hearbeat = a.options.heartbeat_interval;

  for (int i = 0; i < *a.options.max_retransmissions; ++i) {
    RTC_LOG(LS_INFO) << "Letting HEARTBEAT interval timer expire - sending...";
    AdvanceTime(a, *z, time_to_next_hearbeat);

    // Dropping every heartbeat.
    ASSERT_HAS_VALUE_AND_ASSIGN(SctpPacket hb_packet,
                                SctpPacket::Parse(a.cb.ConsumeSentPacket()));
    EXPECT_EQ(hb_packet.descriptors()[0].type, HeartbeatRequestChunk::kType);

    RTC_LOG(LS_INFO) << "Letting the heartbeat expire.";
    AdvanceTime(a, *z, DurationMs(1000));

    time_to_next_hearbeat = a.options.heartbeat_interval - DurationMs(1000);
  }

  RTC_LOG(LS_INFO) << "Letting HEARTBEAT interval timer expire - sending...";
  AdvanceTime(a, *z, time_to_next_hearbeat);

  // Last heartbeat
  EXPECT_THAT(a.cb.ConsumeSentPacket(), Not(IsEmpty()));

  EXPECT_CALL(a.cb, OnAborted).Times(1);
  // Should suffice as exceeding RTO
  AdvanceTime(a, *z, DurationMs(1000));

  z = MaybeHandoverSocket(std::move(z));
}

TEST_P(DcSctpSocketParametrizedTest, RecoversAfterASuccessfulAck) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  EXPECT_THAT(a.cb.ConsumeSentPacket(), testing::IsEmpty());
  EXPECT_CALL(z->cb, OnClosed).Times(1);
  // Force-close socket Z so that it doesn't interfere from now on.
  z->socket.Close();

  DurationMs time_to_next_hearbeat = a.options.heartbeat_interval;

  for (int i = 0; i < *a.options.max_retransmissions; ++i) {
    AdvanceTime(a, *z, time_to_next_hearbeat);

    // Dropping every heartbeat.
    a.cb.ConsumeSentPacket();

    RTC_LOG(LS_INFO) << "Letting the heartbeat expire.";
    AdvanceTime(a, *z, DurationMs(1000));

    time_to_next_hearbeat = a.options.heartbeat_interval - DurationMs(1000);
  }

  RTC_LOG(LS_INFO) << "Getting the last heartbeat - and acking it";
  AdvanceTime(a, *z, time_to_next_hearbeat);

  std::vector<uint8_t> hb_packet_raw = a.cb.ConsumeSentPacket();
  ASSERT_HAS_VALUE_AND_ASSIGN(SctpPacket hb_packet,
                              SctpPacket::Parse(hb_packet_raw));
  ASSERT_THAT(hb_packet.descriptors(), SizeIs(1));
  ASSERT_HAS_VALUE_AND_ASSIGN(
      HeartbeatRequestChunk hb,
      HeartbeatRequestChunk::Parse(hb_packet.descriptors()[0].data));

  SctpPacket::Builder b(a.socket.verification_tag(), a.options);
  b.Add(HeartbeatAckChunk(std::move(hb).extract_parameters()));
  a.socket.ReceivePacket(b.Build());

  // Should suffice as exceeding RTO - which will not fire.
  EXPECT_CALL(a.cb, OnAborted).Times(0);
  AdvanceTime(a, *z, DurationMs(1000));

  EXPECT_THAT(a.cb.ConsumeSentPacket(), IsEmpty());

  // Verify that we get new heartbeats again.
  RTC_LOG(LS_INFO) << "Expecting a new heartbeat";
  AdvanceTime(a, *z, time_to_next_hearbeat);

  ASSERT_HAS_VALUE_AND_ASSIGN(SctpPacket another_packet,
                              SctpPacket::Parse(a.cb.ConsumeSentPacket()));
  EXPECT_EQ(another_packet.descriptors()[0].type, HeartbeatRequestChunk::kType);
}

TEST_P(DcSctpSocketParametrizedTest, ResetStream) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), {1, 2}), {});
  z->socket.ReceivePacket(a.cb.ConsumeSentPacket());

  absl::optional<DcSctpMessage> msg = z->cb.ConsumeReceivedMessage();
  ASSERT_TRUE(msg.has_value());
  EXPECT_EQ(msg->stream_id(), StreamID(1));

  // Handle SACK
  a.socket.ReceivePacket(z->cb.ConsumeSentPacket());

  // Reset the outgoing stream. This will directly send a RE-CONFIG.
  a.socket.ResetStreams(std::vector<StreamID>({StreamID(1)}));

  // Receiving the packet will trigger a callback, indicating that A has
  // reset its stream. It will also send a RE-CONFIG with a response.
  EXPECT_CALL(z->cb, OnIncomingStreamsReset).Times(1);
  z->socket.ReceivePacket(a.cb.ConsumeSentPacket());

  // Receiving a response will trigger a callback. Streams are now reset.
  EXPECT_CALL(a.cb, OnStreamsResetPerformed).Times(1);
  a.socket.ReceivePacket(z->cb.ConsumeSentPacket());

  MaybeHandoverSocketAndSendMessage(a, std::move(z));
}

TEST_P(DcSctpSocketParametrizedTest, ResetStreamWillMakeChunksStartAtZeroSsn) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  std::vector<uint8_t> payload(a.options.mtu - 100);

  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), payload), {});
  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), payload), {});

  auto packet1 = a.cb.ConsumeSentPacket();
  EXPECT_THAT(packet1, HasDataChunkWithSsn(SSN(0)));
  z->socket.ReceivePacket(packet1);

  auto packet2 = a.cb.ConsumeSentPacket();
  EXPECT_THAT(packet2, HasDataChunkWithSsn(SSN(1)));
  z->socket.ReceivePacket(packet2);

  // Handle SACK
  a.socket.ReceivePacket(z->cb.ConsumeSentPacket());

  absl::optional<DcSctpMessage> msg1 = z->cb.ConsumeReceivedMessage();
  ASSERT_TRUE(msg1.has_value());
  EXPECT_EQ(msg1->stream_id(), StreamID(1));

  absl::optional<DcSctpMessage> msg2 = z->cb.ConsumeReceivedMessage();
  ASSERT_TRUE(msg2.has_value());
  EXPECT_EQ(msg2->stream_id(), StreamID(1));

  // Reset the outgoing stream. This will directly send a RE-CONFIG.
  a.socket.ResetStreams(std::vector<StreamID>({StreamID(1)}));
  // RE-CONFIG, req
  z->socket.ReceivePacket(a.cb.ConsumeSentPacket());
  // RE-CONFIG, resp
  a.socket.ReceivePacket(z->cb.ConsumeSentPacket());

  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), payload), {});

  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), payload), {});

  auto packet3 = a.cb.ConsumeSentPacket();
  EXPECT_THAT(packet3, HasDataChunkWithSsn(SSN(0)));
  z->socket.ReceivePacket(packet3);

  auto packet4 = a.cb.ConsumeSentPacket();
  EXPECT_THAT(packet4, HasDataChunkWithSsn(SSN(1)));
  z->socket.ReceivePacket(packet4);

  // Handle SACK
  a.socket.ReceivePacket(z->cb.ConsumeSentPacket());

  MaybeHandoverSocketAndSendMessage(a, std::move(z));
}

TEST_P(DcSctpSocketParametrizedTest,
       ResetStreamWillOnlyResetTheRequestedStreams) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  std::vector<uint8_t> payload(a.options.mtu - 100);

  // Send two ordered messages on SID 1
  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), payload), {});
  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), payload), {});

  auto packet1 = a.cb.ConsumeSentPacket();
  EXPECT_THAT(packet1, HasDataChunkWithStreamId(StreamID(1)));
  EXPECT_THAT(packet1, HasDataChunkWithSsn(SSN(0)));
  z->socket.ReceivePacket(packet1);

  auto packet2 = a.cb.ConsumeSentPacket();
  EXPECT_THAT(packet1, HasDataChunkWithStreamId(StreamID(1)));
  EXPECT_THAT(packet2, HasDataChunkWithSsn(SSN(1)));
  z->socket.ReceivePacket(packet2);

  // Handle SACK
  a.socket.ReceivePacket(z->cb.ConsumeSentPacket());

  // Do the same, for SID 3
  a.socket.Send(DcSctpMessage(StreamID(3), PPID(53), payload), {});
  a.socket.Send(DcSctpMessage(StreamID(3), PPID(53), payload), {});
  auto packet3 = a.cb.ConsumeSentPacket();
  EXPECT_THAT(packet3, HasDataChunkWithStreamId(StreamID(3)));
  EXPECT_THAT(packet3, HasDataChunkWithSsn(SSN(0)));
  z->socket.ReceivePacket(packet3);
  auto packet4 = a.cb.ConsumeSentPacket();
  EXPECT_THAT(packet4, HasDataChunkWithStreamId(StreamID(3)));
  EXPECT_THAT(packet4, HasDataChunkWithSsn(SSN(1)));
  z->socket.ReceivePacket(packet4);
  a.socket.ReceivePacket(z->cb.ConsumeSentPacket());

  // Receive all messages.
  absl::optional<DcSctpMessage> msg1 = z->cb.ConsumeReceivedMessage();
  ASSERT_TRUE(msg1.has_value());
  EXPECT_EQ(msg1->stream_id(), StreamID(1));

  absl::optional<DcSctpMessage> msg2 = z->cb.ConsumeReceivedMessage();
  ASSERT_TRUE(msg2.has_value());
  EXPECT_EQ(msg2->stream_id(), StreamID(1));

  absl::optional<DcSctpMessage> msg3 = z->cb.ConsumeReceivedMessage();
  ASSERT_TRUE(msg3.has_value());
  EXPECT_EQ(msg3->stream_id(), StreamID(3));

  absl::optional<DcSctpMessage> msg4 = z->cb.ConsumeReceivedMessage();
  ASSERT_TRUE(msg4.has_value());
  EXPECT_EQ(msg4->stream_id(), StreamID(3));

  // Reset SID 1. This will directly send a RE-CONFIG.
  a.socket.ResetStreams(std::vector<StreamID>({StreamID(3)}));
  // RE-CONFIG, req
  z->socket.ReceivePacket(a.cb.ConsumeSentPacket());
  // RE-CONFIG, resp
  a.socket.ReceivePacket(z->cb.ConsumeSentPacket());

  // Send a message on SID 1 and 3 - SID 1 should not be reset, but 3 should.
  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), payload), {});

  a.socket.Send(DcSctpMessage(StreamID(3), PPID(53), payload), {});

  auto packet5 = a.cb.ConsumeSentPacket();
  EXPECT_THAT(packet5, HasDataChunkWithStreamId(StreamID(1)));
  EXPECT_THAT(packet5, HasDataChunkWithSsn(SSN(2)));  // Unchanged.
  z->socket.ReceivePacket(packet5);

  auto packet6 = a.cb.ConsumeSentPacket();
  EXPECT_THAT(packet6, HasDataChunkWithStreamId(StreamID(3)));
  EXPECT_THAT(packet6, HasDataChunkWithSsn(SSN(0)));  // Reset.
  z->socket.ReceivePacket(packet6);

  // Handle SACK
  a.socket.ReceivePacket(z->cb.ConsumeSentPacket());

  MaybeHandoverSocketAndSendMessage(a, std::move(z));
}

TEST_P(DcSctpSocketParametrizedTest, OnePeerReconnects) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  EXPECT_CALL(a.cb, OnConnectionRestarted).Times(1);
  // Let's be evil here - reconnect while a fragmented packet was about to be
  // sent. The receiving side should get it in full.
  std::vector<uint8_t> payload(kLargeMessageSize);
  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), payload), kSendOptions);

  // First DATA
  z->socket.ReceivePacket(a.cb.ConsumeSentPacket());

  // Create a new association, z2 - and don't use z anymore.
  SocketUnderTest z2("Z2");
  z2.socket.Connect();

  // Retransmit and handle the rest. As there will be some chunks in-flight that
  // have the wrong verification tag, those will yield errors.
  ExchangeMessages(a, z2);

  absl::optional<DcSctpMessage> msg = z2.cb.ConsumeReceivedMessage();
  ASSERT_TRUE(msg.has_value());
  EXPECT_EQ(msg->stream_id(), StreamID(1));
  EXPECT_THAT(msg->payload(), testing::ElementsAreArray(payload));
}

TEST_P(DcSctpSocketParametrizedTest, SendMessageWithLimitedRtx) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  SendOptions send_options;
  send_options.max_retransmissions = 0;
  std::vector<uint8_t> payload(a.options.mtu - 100);
  a.socket.Send(DcSctpMessage(StreamID(1), PPID(51), payload), send_options);
  a.socket.Send(DcSctpMessage(StreamID(1), PPID(52), payload), send_options);
  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), payload), send_options);

  // First DATA
  z->socket.ReceivePacket(a.cb.ConsumeSentPacket());
  // Second DATA (lost)
  a.cb.ConsumeSentPacket();
  // Third DATA
  z->socket.ReceivePacket(a.cb.ConsumeSentPacket());

  // Handle SACK for first DATA
  a.socket.ReceivePacket(z->cb.ConsumeSentPacket());

  // Handle delayed SACK for third DATA
  AdvanceTime(a, *z, a.options.delayed_ack_max_timeout);

  // Handle SACK for second DATA
  a.socket.ReceivePacket(z->cb.ConsumeSentPacket());

  // Now the missing data chunk will be marked as nacked, but it might still be
  // in-flight and the reported gap could be due to out-of-order delivery. So
  // the RetransmissionQueue will not mark it as "to be retransmitted" until
  // after the t3-rtx timer has expired.
  AdvanceTime(a, *z, a.options.rto_initial);

  // The chunk will be marked as retransmitted, and then as abandoned, which
  // will trigger a FORWARD-TSN to be sent.

  // FORWARD-TSN (third)
  z->socket.ReceivePacket(a.cb.ConsumeSentPacket());

  // Which will trigger a SACK
  a.socket.ReceivePacket(z->cb.ConsumeSentPacket());

  absl::optional<DcSctpMessage> msg1 = z->cb.ConsumeReceivedMessage();
  ASSERT_TRUE(msg1.has_value());
  EXPECT_EQ(msg1->ppid(), PPID(51));

  absl::optional<DcSctpMessage> msg2 = z->cb.ConsumeReceivedMessage();
  ASSERT_TRUE(msg2.has_value());
  EXPECT_EQ(msg2->ppid(), PPID(53));

  absl::optional<DcSctpMessage> msg3 = z->cb.ConsumeReceivedMessage();
  EXPECT_FALSE(msg3.has_value());

  MaybeHandoverSocketAndSendMessage(a, std::move(z));
}

TEST_P(DcSctpSocketParametrizedTest, SendManyFragmentedMessagesWithLimitedRtx) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  SendOptions send_options;
  send_options.unordered = IsUnordered(true);
  send_options.max_retransmissions = 0;
  std::vector<uint8_t> payload(a.options.mtu * 2 - 100 /* margin */);
  // Sending first message
  a.socket.Send(DcSctpMessage(StreamID(1), PPID(51), payload), send_options);
  // Sending second message
  a.socket.Send(DcSctpMessage(StreamID(1), PPID(52), payload), send_options);
  // Sending third message
  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), payload), send_options);
  // Sending fourth message
  a.socket.Send(DcSctpMessage(StreamID(1), PPID(54), payload), send_options);

  // First DATA, first fragment
  std::vector<uint8_t> packet = a.cb.ConsumeSentPacket();
  EXPECT_THAT(packet, HasDataChunkWithPPID(PPID(51)));
  z->socket.ReceivePacket(std::move(packet));

  // First DATA, second fragment (lost)
  packet = a.cb.ConsumeSentPacket();
  EXPECT_THAT(packet, HasDataChunkWithPPID(PPID(51)));

  // Second DATA, first fragment
  packet = a.cb.ConsumeSentPacket();
  EXPECT_THAT(packet, HasDataChunkWithPPID(PPID(52)));
  z->socket.ReceivePacket(std::move(packet));

  // Second DATA, second fragment (lost)
  packet = a.cb.ConsumeSentPacket();
  EXPECT_THAT(packet, HasDataChunkWithPPID(PPID(52)));
  EXPECT_THAT(packet, HasDataChunkWithSsn(SSN(0)));

  // Third DATA, first fragment
  packet = a.cb.ConsumeSentPacket();
  EXPECT_THAT(packet, HasDataChunkWithPPID(PPID(53)));
  EXPECT_THAT(packet, HasDataChunkWithSsn(SSN(0)));
  z->socket.ReceivePacket(std::move(packet));

  // Third DATA, second fragment (lost)
  packet = a.cb.ConsumeSentPacket();
  EXPECT_THAT(packet, HasDataChunkWithPPID(PPID(53)));
  EXPECT_THAT(packet, HasDataChunkWithSsn(SSN(0)));

  // Fourth DATA, first fragment
  packet = a.cb.ConsumeSentPacket();
  EXPECT_THAT(packet, HasDataChunkWithPPID(PPID(54)));
  EXPECT_THAT(packet, HasDataChunkWithSsn(SSN(0)));
  z->socket.ReceivePacket(std::move(packet));

  // Fourth DATA, second fragment
  packet = a.cb.ConsumeSentPacket();
  EXPECT_THAT(packet, HasDataChunkWithPPID(PPID(54)));
  EXPECT_THAT(packet, HasDataChunkWithSsn(SSN(0)));
  z->socket.ReceivePacket(std::move(packet));

  ExchangeMessages(a, *z);

  // Let the RTX timer expire, and exchange FORWARD-TSN/SACKs
  AdvanceTime(a, *z, a.options.rto_initial);

  ExchangeMessages(a, *z);

  absl::optional<DcSctpMessage> msg1 = z->cb.ConsumeReceivedMessage();
  ASSERT_TRUE(msg1.has_value());
  EXPECT_EQ(msg1->ppid(), PPID(54));

  ASSERT_FALSE(z->cb.ConsumeReceivedMessage().has_value());

  MaybeHandoverSocketAndSendMessage(a, std::move(z));
}

struct FakeChunkConfig : ChunkConfig {
  static constexpr int kType = 0x49;
  static constexpr size_t kHeaderSize = 4;
  static constexpr int kVariableLengthAlignment = 0;
};

class FakeChunk : public Chunk, public TLVTrait<FakeChunkConfig> {
 public:
  FakeChunk() {}

  FakeChunk(FakeChunk&& other) = default;
  FakeChunk& operator=(FakeChunk&& other) = default;

  void SerializeTo(std::vector<uint8_t>& out) const override {
    AllocateTLV(out);
  }
  std::string ToString() const override { return "FAKE"; }
};

TEST_P(DcSctpSocketParametrizedTest, ReceivingUnknownChunkRespondsWithError) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  // Inject a FAKE chunk
  SctpPacket::Builder b(a.socket.verification_tag(), DcSctpOptions());
  b.Add(FakeChunk());
  a.socket.ReceivePacket(b.Build());

  // ERROR is sent as a reply. Capture it.
  ASSERT_HAS_VALUE_AND_ASSIGN(SctpPacket reply_packet,
                              SctpPacket::Parse(a.cb.ConsumeSentPacket()));
  ASSERT_THAT(reply_packet.descriptors(), SizeIs(1));
  ASSERT_HAS_VALUE_AND_ASSIGN(
      ErrorChunk error, ErrorChunk::Parse(reply_packet.descriptors()[0].data));
  ASSERT_HAS_VALUE_AND_ASSIGN(
      UnrecognizedChunkTypeCause cause,
      error.error_causes().get<UnrecognizedChunkTypeCause>());
  EXPECT_THAT(cause.unrecognized_chunk(), ElementsAre(0x49, 0x00, 0x00, 0x04));

  MaybeHandoverSocketAndSendMessage(a, std::move(z));
}

TEST_P(DcSctpSocketParametrizedTest, ReceivingErrorChunkReportsAsCallback) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  // Inject a ERROR chunk
  SctpPacket::Builder b(a.socket.verification_tag(), DcSctpOptions());
  b.Add(
      ErrorChunk(Parameters::Builder()
                     .Add(UnrecognizedChunkTypeCause({0x49, 0x00, 0x00, 0x04}))
                     .Build()));

  EXPECT_CALL(a.cb, OnError(ErrorKind::kPeerReported,
                            HasSubstr("Unrecognized Chunk Type")));
  a.socket.ReceivePacket(b.Build());

  MaybeHandoverSocketAndSendMessage(a, std::move(z));
}

TEST(DcSctpSocketTest, PassingHighWatermarkWillOnlyAcceptCumAckTsn) {
  SocketUnderTest a("A");

  constexpr size_t kReceiveWindowBufferSize = 2000;
  SocketUnderTest z(
      "Z", {.mtu = 3000,
            .max_receiver_window_buffer_size = kReceiveWindowBufferSize});

  EXPECT_CALL(z.cb, OnClosed).Times(0);
  EXPECT_CALL(z.cb, OnAborted).Times(0);

  a.socket.Connect();
  std::vector<uint8_t> init_data = a.cb.ConsumeSentPacket();
  ASSERT_HAS_VALUE_AND_ASSIGN(SctpPacket init_packet,
                              SctpPacket::Parse(init_data));
  ASSERT_HAS_VALUE_AND_ASSIGN(
      InitChunk init_chunk,
      InitChunk::Parse(init_packet.descriptors()[0].data));
  z.socket.ReceivePacket(init_data);
  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());
  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());

  // Fill up Z2 to the high watermark limit.
  constexpr size_t kWatermarkLimit =
      kReceiveWindowBufferSize * ReassemblyQueue::kHighWatermarkLimit;
  constexpr size_t kRemainingSize = kReceiveWindowBufferSize - kWatermarkLimit;

  TSN tsn = init_chunk.initial_tsn();
  AnyDataChunk::Options opts;
  opts.is_beginning = Data::IsBeginning(true);
  z.socket.ReceivePacket(
      SctpPacket::Builder(z.socket.verification_tag(), z.options)
          .Add(DataChunk(tsn, StreamID(1), SSN(0), PPID(53),
                         std::vector<uint8_t>(kWatermarkLimit + 1), opts))
          .Build());

  // First DATA will always trigger a SACK. It's not interesting.
  EXPECT_THAT(z.cb.ConsumeSentPacket(),
              AllOf(HasSackWithCumAckTsn(tsn), HasSackWithNoGapAckBlocks()));

  // This DATA should be accepted - it's advancing cum ack tsn.
  z.socket.ReceivePacket(
      SctpPacket::Builder(z.socket.verification_tag(), z.options)
          .Add(DataChunk(AddTo(tsn, 1), StreamID(1), SSN(0), PPID(53),
                         std::vector<uint8_t>(1),
                         /*options=*/{}))
          .Build());

  // The receiver might have moved into delayed ack mode.
  AdvanceTime(a, z, z.options.rto_initial);

  EXPECT_THAT(
      z.cb.ConsumeSentPacket(),
      AllOf(HasSackWithCumAckTsn(AddTo(tsn, 1)), HasSackWithNoGapAckBlocks()));

  // This DATA will not be accepted - it's not advancing cum ack tsn.
  z.socket.ReceivePacket(
      SctpPacket::Builder(z.socket.verification_tag(), z.options)
          .Add(DataChunk(AddTo(tsn, 3), StreamID(1), SSN(0), PPID(53),
                         std::vector<uint8_t>(1),
                         /*options=*/{}))
          .Build());

  // Sack will be sent in IMMEDIATE mode when this is happening.
  EXPECT_THAT(
      z.cb.ConsumeSentPacket(),
      AllOf(HasSackWithCumAckTsn(AddTo(tsn, 1)), HasSackWithNoGapAckBlocks()));

  // This DATA will not be accepted either.
  z.socket.ReceivePacket(
      SctpPacket::Builder(z.socket.verification_tag(), z.options)
          .Add(DataChunk(AddTo(tsn, 4), StreamID(1), SSN(0), PPID(53),
                         std::vector<uint8_t>(1),
                         /*options=*/{}))
          .Build());

  // Sack will be sent in IMMEDIATE mode when this is happening.
  EXPECT_THAT(
      z.cb.ConsumeSentPacket(),
      AllOf(HasSackWithCumAckTsn(AddTo(tsn, 1)), HasSackWithNoGapAckBlocks()));

  // This DATA should be accepted, and it fills the reassembly queue.
  z.socket.ReceivePacket(
      SctpPacket::Builder(z.socket.verification_tag(), z.options)
          .Add(DataChunk(AddTo(tsn, 2), StreamID(1), SSN(0), PPID(53),
                         std::vector<uint8_t>(kRemainingSize),
                         /*options=*/{}))
          .Build());

  // The receiver might have moved into delayed ack mode.
  AdvanceTime(a, z, z.options.rto_initial);

  EXPECT_THAT(
      z.cb.ConsumeSentPacket(),
      AllOf(HasSackWithCumAckTsn(AddTo(tsn, 2)), HasSackWithNoGapAckBlocks()));

  EXPECT_CALL(z.cb, OnAborted(ErrorKind::kResourceExhaustion, _));
  EXPECT_CALL(z.cb, OnClosed).Times(0);

  // This DATA will make the connection close. It's too full now.
  z.socket.ReceivePacket(
      SctpPacket::Builder(z.socket.verification_tag(), z.options)
          .Add(DataChunk(AddTo(tsn, 3), StreamID(1), SSN(0), PPID(53),
                         std::vector<uint8_t>(kSmallMessageSize),
                         /*options=*/{}))
          .Build());
}

TEST(DcSctpSocketTest, SetMaxMessageSize) {
  SocketUnderTest a("A");

  a.socket.SetMaxMessageSize(42u);
  EXPECT_EQ(a.socket.options().max_message_size, 42u);
}

TEST_P(DcSctpSocketParametrizedTest, SendsMessagesWithLowLifetime) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  // Mock that the time always goes forward.
  TimeMs now(0);
  EXPECT_CALL(a.cb, TimeMillis).WillRepeatedly([&]() {
    now += DurationMs(3);
    return now;
  });
  EXPECT_CALL(z->cb, TimeMillis).WillRepeatedly([&]() {
    now += DurationMs(3);
    return now;
  });

  // Queue a few small messages with low lifetime, both ordered and unordered,
  // and validate that all are delivered.
  static constexpr int kIterations = 100;
  for (int i = 0; i < kIterations; ++i) {
    SendOptions send_options;
    send_options.unordered = IsUnordered((i % 2) == 0);
    send_options.lifetime = DurationMs(i % 3);  // 0, 1, 2 ms

    a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), {1, 2}), send_options);
  }

  ExchangeMessages(a, *z);

  for (int i = 0; i < kIterations; ++i) {
    EXPECT_TRUE(z->cb.ConsumeReceivedMessage().has_value());
  }

  EXPECT_FALSE(z->cb.ConsumeReceivedMessage().has_value());

  // Validate that the sockets really make the time move forward.
  EXPECT_GE(*now, kIterations * 2);

  MaybeHandoverSocketAndSendMessage(a, std::move(z));
}

TEST_P(DcSctpSocketParametrizedTest,
       DiscardsMessagesWithLowLifetimeIfMustBuffer) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  SendOptions lifetime_0;
  lifetime_0.unordered = IsUnordered(true);
  lifetime_0.lifetime = DurationMs(0);

  SendOptions lifetime_1;
  lifetime_1.unordered = IsUnordered(true);
  lifetime_1.lifetime = DurationMs(1);

  // Mock that the time always goes forward.
  TimeMs now(0);
  EXPECT_CALL(a.cb, TimeMillis).WillRepeatedly([&]() {
    now += DurationMs(3);
    return now;
  });
  EXPECT_CALL(z->cb, TimeMillis).WillRepeatedly([&]() {
    now += DurationMs(3);
    return now;
  });

  // Fill up the send buffer with a large message.
  std::vector<uint8_t> payload(kLargeMessageSize);
  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), payload), kSendOptions);

  // And queue a few small messages with lifetime=0 or 1 ms - can't be sent.
  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), {1, 2, 3}), lifetime_0);
  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), {4, 5, 6}), lifetime_1);
  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), {7, 8, 9}), lifetime_0);

  // Handle all that was sent until congestion window got full.
  for (;;) {
    std::vector<uint8_t> packet_from_a = a.cb.ConsumeSentPacket();
    if (packet_from_a.empty()) {
      break;
    }
    z->socket.ReceivePacket(std::move(packet_from_a));
  }

  // Shouldn't be enough to send that large message.
  EXPECT_FALSE(z->cb.ConsumeReceivedMessage().has_value());

  // Exchange the rest of the messages, with the time ever increasing.
  ExchangeMessages(a, *z);

  // The large message should be delivered. It was sent reliably.
  ASSERT_HAS_VALUE_AND_ASSIGN(DcSctpMessage m1, z->cb.ConsumeReceivedMessage());
  EXPECT_EQ(m1.stream_id(), StreamID(1));
  EXPECT_THAT(m1.payload(), SizeIs(kLargeMessageSize));

  // But none of the smaller messages.
  EXPECT_FALSE(z->cb.ConsumeReceivedMessage().has_value());

  MaybeHandoverSocketAndSendMessage(a, std::move(z));
}

TEST_P(DcSctpSocketParametrizedTest, HasReasonableBufferedAmountValues) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  EXPECT_EQ(a.socket.buffered_amount(StreamID(1)), 0u);

  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53),
                              std::vector<uint8_t>(kSmallMessageSize)),
                kSendOptions);
  // Sending a small message will directly send it as a single packet, so
  // nothing is left in the queue.
  EXPECT_EQ(a.socket.buffered_amount(StreamID(1)), 0u);

  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53),
                              std::vector<uint8_t>(kLargeMessageSize)),
                kSendOptions);

  // Sending a message will directly start sending a few packets, so the
  // buffered amount is not the full message size.
  EXPECT_GT(a.socket.buffered_amount(StreamID(1)), 0u);
  EXPECT_LT(a.socket.buffered_amount(StreamID(1)), kLargeMessageSize);

  MaybeHandoverSocketAndSendMessage(a, std::move(z));
}

TEST(DcSctpSocketTest, HasDefaultOnBufferedAmountLowValueZero) {
  SocketUnderTest a("A");
  EXPECT_EQ(a.socket.buffered_amount_low_threshold(StreamID(1)), 0u);
}

TEST_P(DcSctpSocketParametrizedTest,
       TriggersOnBufferedAmountLowWithDefaultValueZero) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  EXPECT_CALL(a.cb, OnBufferedAmountLow).Times(0);
  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  EXPECT_CALL(a.cb, OnBufferedAmountLow(StreamID(1)));
  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53),
                              std::vector<uint8_t>(kSmallMessageSize)),
                kSendOptions);
  ExchangeMessages(a, *z);

  EXPECT_CALL(a.cb, OnBufferedAmountLow).WillRepeatedly(testing::Return());
  MaybeHandoverSocketAndSendMessage(a, std::move(z));
}

TEST_P(DcSctpSocketParametrizedTest,
       DoesntTriggerOnBufferedAmountLowIfBelowThreshold) {
  static constexpr size_t kMessageSize = 1000;
  static constexpr size_t kBufferedAmountLowThreshold = kMessageSize * 10;

  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  a.socket.SetBufferedAmountLowThreshold(StreamID(1),
                                         kBufferedAmountLowThreshold);
  EXPECT_CALL(a.cb, OnBufferedAmountLow).Times(0);
  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  EXPECT_CALL(a.cb, OnBufferedAmountLow(StreamID(1))).Times(0);
  a.socket.Send(
      DcSctpMessage(StreamID(1), PPID(53), std::vector<uint8_t>(kMessageSize)),
      kSendOptions);
  ExchangeMessages(a, *z);

  a.socket.Send(
      DcSctpMessage(StreamID(1), PPID(53), std::vector<uint8_t>(kMessageSize)),
      kSendOptions);
  ExchangeMessages(a, *z);

  MaybeHandoverSocketAndSendMessage(a, std::move(z));
}

TEST_P(DcSctpSocketParametrizedTest, TriggersOnBufferedAmountMultipleTimes) {
  static constexpr size_t kMessageSize = 1000;
  static constexpr size_t kBufferedAmountLowThreshold = kMessageSize / 2;

  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  a.socket.SetBufferedAmountLowThreshold(StreamID(1),
                                         kBufferedAmountLowThreshold);
  EXPECT_CALL(a.cb, OnBufferedAmountLow).Times(0);
  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  EXPECT_CALL(a.cb, OnBufferedAmountLow(StreamID(1))).Times(3);
  EXPECT_CALL(a.cb, OnBufferedAmountLow(StreamID(2))).Times(2);
  a.socket.Send(
      DcSctpMessage(StreamID(1), PPID(53), std::vector<uint8_t>(kMessageSize)),
      kSendOptions);
  ExchangeMessages(a, *z);

  a.socket.Send(
      DcSctpMessage(StreamID(2), PPID(53), std::vector<uint8_t>(kMessageSize)),
      kSendOptions);
  ExchangeMessages(a, *z);

  a.socket.Send(
      DcSctpMessage(StreamID(1), PPID(53), std::vector<uint8_t>(kMessageSize)),
      kSendOptions);
  ExchangeMessages(a, *z);

  a.socket.Send(
      DcSctpMessage(StreamID(2), PPID(53), std::vector<uint8_t>(kMessageSize)),
      kSendOptions);
  ExchangeMessages(a, *z);

  a.socket.Send(
      DcSctpMessage(StreamID(1), PPID(53), std::vector<uint8_t>(kMessageSize)),
      kSendOptions);
  ExchangeMessages(a, *z);

  MaybeHandoverSocketAndSendMessage(a, std::move(z));
}

TEST_P(DcSctpSocketParametrizedTest,
       TriggersOnBufferedAmountLowOnlyWhenCrossingThreshold) {
  static constexpr size_t kMessageSize = 1000;
  static constexpr size_t kBufferedAmountLowThreshold = kMessageSize * 1.5;

  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  a.socket.SetBufferedAmountLowThreshold(StreamID(1),
                                         kBufferedAmountLowThreshold);
  EXPECT_CALL(a.cb, OnBufferedAmountLow).Times(0);
  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  EXPECT_CALL(a.cb, OnBufferedAmountLow).Times(0);

  // Add a few messages to fill up the congestion window. When that is full,
  // messages will start to be fully buffered.
  while (a.socket.buffered_amount(StreamID(1)) <= kBufferedAmountLowThreshold) {
    a.socket.Send(DcSctpMessage(StreamID(1), PPID(53),
                                std::vector<uint8_t>(kMessageSize)),
                  kSendOptions);
  }
  size_t initial_buffered = a.socket.buffered_amount(StreamID(1));
  ASSERT_GT(initial_buffered, kBufferedAmountLowThreshold);

  // Start ACKing packets, which will empty the send queue, and trigger the
  // callback.
  EXPECT_CALL(a.cb, OnBufferedAmountLow(StreamID(1))).Times(1);
  ExchangeMessages(a, *z);

  MaybeHandoverSocketAndSendMessage(a, std::move(z));
}

TEST_P(DcSctpSocketParametrizedTest,
       DoesntTriggerOnTotalBufferAmountLowWhenBelow) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  EXPECT_CALL(a.cb, OnTotalBufferedAmountLow).Times(0);

  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53),
                              std::vector<uint8_t>(kLargeMessageSize)),
                kSendOptions);

  ExchangeMessages(a, *z);

  MaybeHandoverSocketAndSendMessage(a, std::move(z));
}

TEST_P(DcSctpSocketParametrizedTest,
       TriggersOnTotalBufferAmountLowWhenCrossingThreshold) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  EXPECT_CALL(a.cb, OnTotalBufferedAmountLow).Times(0);

  // Fill up the send queue completely.
  for (;;) {
    if (a.socket.Send(DcSctpMessage(StreamID(1), PPID(53),
                                    std::vector<uint8_t>(kLargeMessageSize)),
                      kSendOptions) == SendStatus::kErrorResourceExhaustion) {
      break;
    }
  }

  EXPECT_CALL(a.cb, OnTotalBufferedAmountLow).Times(1);
  ExchangeMessages(a, *z);

  MaybeHandoverSocketAndSendMessage(a, std::move(z));
}

TEST(DcSctpSocketTest, InitialMetricsAreZeroed) {
  SocketUnderTest a("A");

  Metrics metrics = a.socket.GetMetrics();
  EXPECT_EQ(metrics.tx_packets_count, 0u);
  EXPECT_EQ(metrics.tx_messages_count, 0u);
  EXPECT_EQ(metrics.cwnd_bytes.has_value(), false);
  EXPECT_EQ(metrics.srtt_ms.has_value(), false);
  EXPECT_EQ(metrics.unack_data_count, 0u);
  EXPECT_EQ(metrics.rx_packets_count, 0u);
  EXPECT_EQ(metrics.rx_messages_count, 0u);
  EXPECT_EQ(metrics.peer_rwnd_bytes.has_value(), false);
}

TEST(DcSctpSocketTest, RxAndTxPacketMetricsIncrease) {
  SocketUnderTest a("A");
  SocketUnderTest z("Z");

  ConnectSockets(a, z);

  const size_t initial_a_rwnd = a.options.max_receiver_window_buffer_size *
                                ReassemblyQueue::kHighWatermarkLimit;

  EXPECT_EQ(a.socket.GetMetrics().tx_packets_count, 2u);
  EXPECT_EQ(a.socket.GetMetrics().rx_packets_count, 2u);
  EXPECT_EQ(a.socket.GetMetrics().tx_messages_count, 0u);
  EXPECT_EQ(*a.socket.GetMetrics().cwnd_bytes,
            a.options.cwnd_mtus_initial * a.options.mtu);
  EXPECT_EQ(a.socket.GetMetrics().unack_data_count, 0u);

  EXPECT_EQ(z.socket.GetMetrics().rx_packets_count, 2u);
  EXPECT_EQ(z.socket.GetMetrics().rx_messages_count, 0u);

  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), {1, 2}), kSendOptions);
  EXPECT_EQ(a.socket.GetMetrics().unack_data_count, 1u);

  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());  // DATA
  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());  // SACK
  EXPECT_EQ(*a.socket.GetMetrics().peer_rwnd_bytes, initial_a_rwnd);
  EXPECT_EQ(a.socket.GetMetrics().unack_data_count, 0u);

  EXPECT_TRUE(z.cb.ConsumeReceivedMessage().has_value());

  EXPECT_EQ(a.socket.GetMetrics().tx_packets_count, 3u);
  EXPECT_EQ(a.socket.GetMetrics().rx_packets_count, 3u);
  EXPECT_EQ(a.socket.GetMetrics().tx_messages_count, 1u);

  EXPECT_EQ(z.socket.GetMetrics().rx_packets_count, 3u);
  EXPECT_EQ(z.socket.GetMetrics().rx_messages_count, 1u);

  // Send one more (large - fragmented), and receive the delayed SACK.
  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53),
                              std::vector<uint8_t>(a.options.mtu * 2 + 1)),
                kSendOptions);
  EXPECT_EQ(a.socket.GetMetrics().unack_data_count, 3u);

  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());  // DATA
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());  // DATA

  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());  // SACK
  EXPECT_EQ(a.socket.GetMetrics().unack_data_count, 1u);
  EXPECT_GT(*a.socket.GetMetrics().peer_rwnd_bytes, 0u);
  EXPECT_LT(*a.socket.GetMetrics().peer_rwnd_bytes, initial_a_rwnd);

  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());  // DATA

  EXPECT_TRUE(z.cb.ConsumeReceivedMessage().has_value());

  EXPECT_EQ(a.socket.GetMetrics().tx_packets_count, 6u);
  EXPECT_EQ(a.socket.GetMetrics().rx_packets_count, 4u);
  EXPECT_EQ(a.socket.GetMetrics().tx_messages_count, 2u);

  EXPECT_EQ(z.socket.GetMetrics().rx_packets_count, 6u);
  EXPECT_EQ(z.socket.GetMetrics().rx_messages_count, 2u);

  // Delayed sack
  AdvanceTime(a, z, a.options.delayed_ack_max_timeout);

  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());  // SACK
  EXPECT_EQ(a.socket.GetMetrics().unack_data_count, 0u);
  EXPECT_EQ(a.socket.GetMetrics().rx_packets_count, 5u);
  EXPECT_EQ(*a.socket.GetMetrics().peer_rwnd_bytes, initial_a_rwnd);
}

TEST_P(DcSctpSocketParametrizedTest, UnackDataAlsoIncludesSendQueue) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53),
                              std::vector<uint8_t>(kLargeMessageSize)),
                kSendOptions);
  size_t payload_bytes =
      a.options.mtu - SctpPacket::kHeaderSize - DataChunk::kHeaderSize;

  size_t expected_sent_packets = a.options.cwnd_mtus_initial;

  size_t expected_queued_bytes =
      kLargeMessageSize - expected_sent_packets * payload_bytes;

  size_t expected_queued_packets = expected_queued_bytes / payload_bytes;

  // Due to alignment, padding etc, it's hard to calculate the exact number, but
  // it should be in this range.
  EXPECT_GE(a.socket.GetMetrics().unack_data_count,
            expected_sent_packets + expected_queued_packets);

  EXPECT_LE(a.socket.GetMetrics().unack_data_count,
            expected_sent_packets + expected_queued_packets + 2);

  MaybeHandoverSocketAndSendMessage(a, std::move(z));
}

TEST_P(DcSctpSocketParametrizedTest, DoesntSendMoreThanMaxBurstPackets) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53),
                              std::vector<uint8_t>(kLargeMessageSize)),
                kSendOptions);

  for (int i = 0; i < kMaxBurstPackets; ++i) {
    std::vector<uint8_t> packet = a.cb.ConsumeSentPacket();
    EXPECT_THAT(packet, Not(IsEmpty()));
    z->socket.ReceivePacket(std::move(packet));  // DATA
  }

  EXPECT_THAT(a.cb.ConsumeSentPacket(), IsEmpty());

  ExchangeMessages(a, *z);
  MaybeHandoverSocketAndSendMessage(a, std::move(z));
}

TEST_P(DcSctpSocketParametrizedTest, SendsOnlyLargePackets) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  // A really large message, to ensure that the congestion window is often full.
  constexpr size_t kMessageSize = 100000;
  a.socket.Send(
      DcSctpMessage(StreamID(1), PPID(53), std::vector<uint8_t>(kMessageSize)),
      kSendOptions);

  bool delivered_packet = false;
  std::vector<size_t> data_packet_sizes;
  do {
    delivered_packet = false;
    std::vector<uint8_t> packet_from_a = a.cb.ConsumeSentPacket();
    if (!packet_from_a.empty()) {
      data_packet_sizes.push_back(packet_from_a.size());
      delivered_packet = true;
      z->socket.ReceivePacket(std::move(packet_from_a));
    }
    std::vector<uint8_t> packet_from_z = z->cb.ConsumeSentPacket();
    if (!packet_from_z.empty()) {
      delivered_packet = true;
      a.socket.ReceivePacket(std::move(packet_from_z));
    }
  } while (delivered_packet);

  size_t packet_payload_bytes =
      a.options.mtu - SctpPacket::kHeaderSize - DataChunk::kHeaderSize;
  // +1 accounts for padding, and rounding up.
  size_t expected_packets =
      (kMessageSize + packet_payload_bytes - 1) / packet_payload_bytes + 1;
  EXPECT_THAT(data_packet_sizes, SizeIs(expected_packets));

  // Remove the last size - it will be the remainder. But all other sizes should
  // be large.
  data_packet_sizes.pop_back();

  for (size_t size : data_packet_sizes) {
    // The 4 is for padding/alignment.
    EXPECT_GE(size, a.options.mtu - 4);
  }

  MaybeHandoverSocketAndSendMessage(a, std::move(z));
}

TEST(DcSctpSocketTest, SendMessagesAfterHandover) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);

  // Send message before handover to move socket to a not initial state
  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), {1, 2}), kSendOptions);
  z->socket.ReceivePacket(a.cb.ConsumeSentPacket());
  z->cb.ConsumeReceivedMessage();

  z = HandoverSocket(std::move(z));

  absl::optional<DcSctpMessage> msg;

  RTC_LOG(LS_INFO) << "Sending A #1";

  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), {3, 4}), kSendOptions);
  z->socket.ReceivePacket(a.cb.ConsumeSentPacket());

  msg = z->cb.ConsumeReceivedMessage();
  ASSERT_TRUE(msg.has_value());
  EXPECT_EQ(msg->stream_id(), StreamID(1));
  EXPECT_THAT(msg->payload(), testing::ElementsAre(3, 4));

  RTC_LOG(LS_INFO) << "Sending A #2";

  a.socket.Send(DcSctpMessage(StreamID(2), PPID(53), {5, 6}), kSendOptions);
  z->socket.ReceivePacket(a.cb.ConsumeSentPacket());

  msg = z->cb.ConsumeReceivedMessage();
  ASSERT_TRUE(msg.has_value());
  EXPECT_EQ(msg->stream_id(), StreamID(2));
  EXPECT_THAT(msg->payload(), testing::ElementsAre(5, 6));

  RTC_LOG(LS_INFO) << "Sending Z #1";

  z->socket.Send(DcSctpMessage(StreamID(1), PPID(53), {1, 2, 3}), kSendOptions);
  a.socket.ReceivePacket(z->cb.ConsumeSentPacket());  // ack
  a.socket.ReceivePacket(z->cb.ConsumeSentPacket());  // data

  msg = a.cb.ConsumeReceivedMessage();
  ASSERT_TRUE(msg.has_value());
  EXPECT_EQ(msg->stream_id(), StreamID(1));
  EXPECT_THAT(msg->payload(), testing::ElementsAre(1, 2, 3));
}

TEST(DcSctpSocketTest, CanDetectDcsctpImplementation) {
  SocketUnderTest a("A");
  SocketUnderTest z("Z");

  ConnectSockets(a, z);

  EXPECT_EQ(a.socket.peer_implementation(), SctpImplementation::kDcsctp);

  // As A initiated the connection establishment, Z will not receive enough
  // information to know about A's implementation
  EXPECT_EQ(z.socket.peer_implementation(), SctpImplementation::kUnknown);
}

TEST(DcSctpSocketTest, BothCanDetectDcsctpImplementation) {
  SocketUnderTest a("A");
  SocketUnderTest z("Z");

  EXPECT_CALL(a.cb, OnConnected).Times(1);
  EXPECT_CALL(z.cb, OnConnected).Times(1);
  a.socket.Connect();
  z.socket.Connect();

  ExchangeMessages(a, z);

  EXPECT_EQ(a.socket.peer_implementation(), SctpImplementation::kDcsctp);
  EXPECT_EQ(z.socket.peer_implementation(), SctpImplementation::kDcsctp);
}

TEST_P(DcSctpSocketParametrizedTest, CanLoseFirstOrderedMessage) {
  SocketUnderTest a("A");
  auto z = std::make_unique<SocketUnderTest>("Z");

  ConnectSockets(a, *z);
  z = MaybeHandoverSocket(std::move(z));

  SendOptions send_options;
  send_options.unordered = IsUnordered(false);
  send_options.max_retransmissions = 0;
  std::vector<uint8_t> payload(a.options.mtu - 100);

  // Send a first message (SID=1, SSN=0)
  a.socket.Send(DcSctpMessage(StreamID(1), PPID(51), payload), send_options);

  // First DATA is lost, and retransmission timer will delete it.
  a.cb.ConsumeSentPacket();
  AdvanceTime(a, *z, a.options.rto_initial);
  ExchangeMessages(a, *z);

  // Send a second message (SID=0, SSN=1).
  a.socket.Send(DcSctpMessage(StreamID(1), PPID(52), payload), send_options);
  ExchangeMessages(a, *z);

  // The Z socket should receive the second message, but not the first.
  absl::optional<DcSctpMessage> msg = z->cb.ConsumeReceivedMessage();
  ASSERT_TRUE(msg.has_value());
  EXPECT_EQ(msg->ppid(), PPID(52));

  EXPECT_FALSE(z->cb.ConsumeReceivedMessage().has_value());

  MaybeHandoverSocketAndSendMessage(a, std::move(z));
}

TEST(DcSctpSocketTest, ReceiveBothUnorderedAndOrderedWithSameTSN) {
  /* This issue was found by fuzzing. */
  SocketUnderTest a("A");
  SocketUnderTest z("Z");

  a.socket.Connect();
  std::vector<uint8_t> init_data = a.cb.ConsumeSentPacket();
  ASSERT_HAS_VALUE_AND_ASSIGN(SctpPacket init_packet,
                              SctpPacket::Parse(init_data));
  ASSERT_HAS_VALUE_AND_ASSIGN(
      InitChunk init_chunk,
      InitChunk::Parse(init_packet.descriptors()[0].data));
  z.socket.ReceivePacket(init_data);
  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());
  z.socket.ReceivePacket(a.cb.ConsumeSentPacket());
  a.socket.ReceivePacket(z.cb.ConsumeSentPacket());

  // Receive a short unordered message with tsn=INITIAL_TSN+1
  TSN tsn = init_chunk.initial_tsn();
  AnyDataChunk::Options opts;
  opts.is_beginning = Data::IsBeginning(true);
  opts.is_end = Data::IsEnd(true);
  opts.is_unordered = IsUnordered(true);
  z.socket.ReceivePacket(
      SctpPacket::Builder(z.socket.verification_tag(), z.options)
          .Add(DataChunk(TSN(*tsn + 1), StreamID(1), SSN(0), PPID(53),
                         std::vector<uint8_t>(10), opts))
          .Build());

  // Now receive a longer _ordered_ message with [INITIAL_TSN, INITIAL_TSN+1].
  // This isn't allowed as it reuses TSN=53 with different properties, but it
  // shouldn't cause any issues.
  opts.is_unordered = IsUnordered(false);
  opts.is_end = Data::IsEnd(false);
  z.socket.ReceivePacket(
      SctpPacket::Builder(z.socket.verification_tag(), z.options)
          .Add(DataChunk(tsn, StreamID(1), SSN(0), PPID(53),
                         std::vector<uint8_t>(10), opts))
          .Build());

  opts.is_beginning = Data::IsBeginning(false);
  opts.is_end = Data::IsEnd(true);
  z.socket.ReceivePacket(
      SctpPacket::Builder(z.socket.verification_tag(), z.options)
          .Add(DataChunk(TSN(*tsn + 1), StreamID(1), SSN(0), PPID(53),
                         std::vector<uint8_t>(10), opts))
          .Build());
}

TEST(DcSctpSocketTest, CloseTwoStreamsAtTheSameTime) {
  // Reported as https://crbug.com/1312009.
  SocketUnderTest a("A");
  SocketUnderTest z("Z");

  EXPECT_CALL(z.cb, OnIncomingStreamsReset(ElementsAre(StreamID(1)))).Times(1);
  EXPECT_CALL(z.cb, OnIncomingStreamsReset(ElementsAre(StreamID(2)))).Times(1);
  EXPECT_CALL(a.cb, OnStreamsResetPerformed(ElementsAre(StreamID(1)))).Times(1);
  EXPECT_CALL(a.cb, OnStreamsResetPerformed(ElementsAre(StreamID(2)))).Times(1);

  ConnectSockets(a, z);

  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), {1, 2}), kSendOptions);
  a.socket.Send(DcSctpMessage(StreamID(2), PPID(53), {1, 2}), kSendOptions);

  ExchangeMessages(a, z);

  a.socket.ResetStreams(std::vector<StreamID>({StreamID(1)}));
  a.socket.ResetStreams(std::vector<StreamID>({StreamID(2)}));

  ExchangeMessages(a, z);
}

TEST(DcSctpSocketTest, CloseThreeStreamsAtTheSameTime) {
  // Similar to CloseTwoStreamsAtTheSameTime, but ensuring that the two
  // remaining streams are reset at the same time in the second request.
  SocketUnderTest a("A");
  SocketUnderTest z("Z");

  EXPECT_CALL(z.cb, OnIncomingStreamsReset(ElementsAre(StreamID(1)))).Times(1);
  EXPECT_CALL(z.cb, OnIncomingStreamsReset(
                        UnorderedElementsAre(StreamID(2), StreamID(3))))
      .Times(1);
  EXPECT_CALL(a.cb, OnStreamsResetPerformed(ElementsAre(StreamID(1)))).Times(1);
  EXPECT_CALL(a.cb, OnStreamsResetPerformed(
                        UnorderedElementsAre(StreamID(2), StreamID(3))))
      .Times(1);

  ConnectSockets(a, z);

  a.socket.Send(DcSctpMessage(StreamID(1), PPID(53), {1, 2}), kSendOptions);
  a.socket.Send(DcSctpMessage(StreamID(2), PPID(53), {1, 2}), kSendOptions);
  a.socket.Send(DcSctpMessage(StreamID(3), PPID(53), {1, 2}), kSendOptions);

  ExchangeMessages(a, z);

  a.socket.ResetStreams(std::vector<StreamID>({StreamID(1)}));
  a.socket.ResetStreams(std::vector<StreamID>({StreamID(2)}));
  a.socket.ResetStreams(std::vector<StreamID>({StreamID(3)}));

  ExchangeMessages(a, z);
}
}  // namespace
}  // namespace dcsctp
