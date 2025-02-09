/*
 *  Copyright 2022 The WebRTC project authors. All Rights Reserved.
 *
 *  Use of this source code is governed by a BSD-style license
 *  that can be found in the LICENSE file in the root of the source
 *  tree. An additional intellectual property rights grant can be found
 *  in the file PATENTS.  All contributing project authors may
 *  be found in the AUTHORS file in the root of the source tree.
 */

#ifndef LOGGING_RTC_EVENT_LOG_EVENTS_LOGGED_RTP_RTCP_H_
#define LOGGING_RTC_EVENT_LOG_EVENTS_LOGGED_RTP_RTCP_H_

#include <string>
#include <vector>

#include "api/rtp_headers.h"
#include "api/units/timestamp.h"
#include "modules/rtp_rtcp/source/rtcp_packet/bye.h"
#include "modules/rtp_rtcp/source/rtcp_packet/extended_reports.h"
#include "modules/rtp_rtcp/source/rtcp_packet/fir.h"
#include "modules/rtp_rtcp/source/rtcp_packet/loss_notification.h"
#include "modules/rtp_rtcp/source/rtcp_packet/nack.h"
#include "modules/rtp_rtcp/source/rtcp_packet/pli.h"
#include "modules/rtp_rtcp/source/rtcp_packet/receiver_report.h"
#include "modules/rtp_rtcp/source/rtcp_packet/remb.h"
#include "modules/rtp_rtcp/source/rtcp_packet/sender_report.h"
#include "modules/rtp_rtcp/source/rtcp_packet/transport_feedback.h"

namespace webrtc {

struct LoggedRtpPacket {
  LoggedRtpPacket(Timestamp timestamp,
                  RTPHeader header,
                  size_t header_length,
                  size_t total_length)
      : timestamp(timestamp),
        header(header),
        header_length(header_length),
        total_length(total_length) {}

  int64_t log_time_us() const { return timestamp.us(); }
  int64_t log_time_ms() const { return timestamp.ms(); }

  Timestamp timestamp;
  // TODO(terelius): This allocates space for 15 CSRCs even if none are used.
  RTPHeader header;
  size_t header_length;
  size_t total_length;
};

struct LoggedRtpPacketIncoming {
  LoggedRtpPacketIncoming(Timestamp timestamp,
                          RTPHeader header,
                          size_t header_length,
                          size_t total_length)
      : rtp(timestamp, header, header_length, total_length) {}
  int64_t log_time_us() const { return rtp.timestamp.us(); }
  int64_t log_time_ms() const { return rtp.timestamp.ms(); }

  LoggedRtpPacket rtp;
};

struct LoggedRtpPacketOutgoing {
  LoggedRtpPacketOutgoing(Timestamp timestamp,
                          RTPHeader header,
                          size_t header_length,
                          size_t total_length)
      : rtp(timestamp, header, header_length, total_length) {}
  int64_t log_time_us() const { return rtp.timestamp.us(); }
  int64_t log_time_ms() const { return rtp.timestamp.ms(); }

  LoggedRtpPacket rtp;
};

struct LoggedRtcpPacket {
  LoggedRtcpPacket(Timestamp timestamp, const std::vector<uint8_t>& packet)
      : timestamp(timestamp), raw_data(packet) {}
  LoggedRtcpPacket(Timestamp timestamp, const std::string& packet)
      : timestamp(timestamp), raw_data(packet.size()) {
    memcpy(raw_data.data(), packet.data(), packet.size());
  }

  LoggedRtcpPacket(const LoggedRtcpPacket& rhs) = default;

  ~LoggedRtcpPacket() = default;

  int64_t log_time_us() const { return timestamp.us(); }
  int64_t log_time_ms() const { return timestamp.ms(); }

  Timestamp timestamp;
  std::vector<uint8_t> raw_data;
};

struct LoggedRtcpPacketIncoming {
  LoggedRtcpPacketIncoming(Timestamp timestamp,
                           const std::vector<uint8_t>& packet)
      : rtcp(timestamp, packet) {}
  LoggedRtcpPacketIncoming(Timestamp timestamp, const std::string& packet)
      : rtcp(timestamp, packet) {}

  int64_t log_time_us() const { return rtcp.timestamp.us(); }
  int64_t log_time_ms() const { return rtcp.timestamp.ms(); }

  LoggedRtcpPacket rtcp;
};

struct LoggedRtcpPacketOutgoing {
  LoggedRtcpPacketOutgoing(Timestamp timestamp,
                           const std::vector<uint8_t>& packet)
      : rtcp(timestamp, packet) {}
  LoggedRtcpPacketOutgoing(Timestamp timestamp, const std::string& packet)
      : rtcp(timestamp, packet) {}

  int64_t log_time_us() const { return rtcp.timestamp.us(); }
  int64_t log_time_ms() const { return rtcp.timestamp.ms(); }

  LoggedRtcpPacket rtcp;
};

struct LoggedRtcpPacketReceiverReport {
  LoggedRtcpPacketReceiverReport() = default;
  LoggedRtcpPacketReceiverReport(Timestamp timestamp,
                                 const rtcp::ReceiverReport& rr)
      : timestamp(timestamp), rr(rr) {}

  int64_t log_time_us() const { return timestamp.us(); }
  int64_t log_time_ms() const { return timestamp.ms(); }

  Timestamp timestamp = Timestamp::MinusInfinity();
  rtcp::ReceiverReport rr;
};

struct LoggedRtcpPacketSenderReport {
  LoggedRtcpPacketSenderReport() = default;
  LoggedRtcpPacketSenderReport(Timestamp timestamp,
                               const rtcp::SenderReport& sr)
      : timestamp(timestamp), sr(sr) {}

  int64_t log_time_us() const { return timestamp.us(); }
  int64_t log_time_ms() const { return timestamp.ms(); }

  Timestamp timestamp = Timestamp::MinusInfinity();
  rtcp::SenderReport sr;
};

struct LoggedRtcpPacketExtendedReports {
  LoggedRtcpPacketExtendedReports() = default;

  int64_t log_time_us() const { return timestamp.us(); }
  int64_t log_time_ms() const { return timestamp.ms(); }

  Timestamp timestamp = Timestamp::MinusInfinity();
  rtcp::ExtendedReports xr;
};

struct LoggedRtcpPacketRemb {
  LoggedRtcpPacketRemb() = default;
  LoggedRtcpPacketRemb(Timestamp timestamp, const rtcp::Remb& remb)
      : timestamp(timestamp), remb(remb) {}

  int64_t log_time_us() const { return timestamp.us(); }
  int64_t log_time_ms() const { return timestamp.ms(); }

  Timestamp timestamp = Timestamp::MinusInfinity();
  rtcp::Remb remb;
};

struct LoggedRtcpPacketNack {
  LoggedRtcpPacketNack() = default;
  LoggedRtcpPacketNack(Timestamp timestamp, const rtcp::Nack& nack)
      : timestamp(timestamp), nack(nack) {}

  int64_t log_time_us() const { return timestamp.us(); }
  int64_t log_time_ms() const { return timestamp.ms(); }

  Timestamp timestamp = Timestamp::MinusInfinity();
  rtcp::Nack nack;
};

struct LoggedRtcpPacketFir {
  LoggedRtcpPacketFir() = default;

  int64_t log_time_us() const { return timestamp.us(); }
  int64_t log_time_ms() const { return timestamp.ms(); }

  Timestamp timestamp = Timestamp::MinusInfinity();
  rtcp::Fir fir;
};

struct LoggedRtcpPacketPli {
  LoggedRtcpPacketPli() = default;

  int64_t log_time_us() const { return timestamp.us(); }
  int64_t log_time_ms() const { return timestamp.ms(); }

  Timestamp timestamp = Timestamp::MinusInfinity();
  rtcp::Pli pli;
};

struct LoggedRtcpPacketTransportFeedback {
  LoggedRtcpPacketTransportFeedback()
      : transport_feedback(/*include_timestamps=*/true, /*include_lost*/ true) {
  }
  LoggedRtcpPacketTransportFeedback(
      Timestamp timestamp,
      const rtcp::TransportFeedback& transport_feedback)
      : timestamp(timestamp), transport_feedback(transport_feedback) {}

  int64_t log_time_us() const { return timestamp.us(); }
  int64_t log_time_ms() const { return timestamp.ms(); }

  Timestamp timestamp = Timestamp::MinusInfinity();
  rtcp::TransportFeedback transport_feedback;
};

struct LoggedRtcpPacketLossNotification {
  LoggedRtcpPacketLossNotification() = default;
  LoggedRtcpPacketLossNotification(
      Timestamp timestamp,
      const rtcp::LossNotification& loss_notification)
      : timestamp(timestamp), loss_notification(loss_notification) {}

  int64_t log_time_us() const { return timestamp.us(); }
  int64_t log_time_ms() const { return timestamp.ms(); }

  Timestamp timestamp = Timestamp::MinusInfinity();
  rtcp::LossNotification loss_notification;
};

struct LoggedRtcpPacketBye {
  LoggedRtcpPacketBye() = default;

  int64_t log_time_us() const { return timestamp.us(); }
  int64_t log_time_ms() const { return timestamp.ms(); }

  Timestamp timestamp = Timestamp::MinusInfinity();
  rtcp::Bye bye;
};

}  // namespace webrtc

#endif  // LOGGING_RTC_EVENT_LOG_EVENTS_LOGGED_RTP_RTCP_H_
