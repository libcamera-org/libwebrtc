/*
 *  Copyright (c) 2022 The WebRTC project authors. All Rights Reserved.
 *
 *  Use of this source code is governed by a BSD-style license
 *  that can be found in the LICENSE file in the root of the source
 *  tree. An additional intellectual property rights grant can be found
 *  in the file PATENTS.  All contributing project authors may
 *  be found in the AUTHORS file in the root of the source tree.
 */

#ifndef VIDEO_FRAME_DECODE_TIMING_H_
#define VIDEO_FRAME_DECODE_TIMING_H_

#include <stdint.h>

#include <functional>

#include "api/task_queue/task_queue_base.h"
#include "modules/video_coding/timing.h"
#include "rtc_base/task_utils/pending_task_safety_flag.h"
#include "system_wrappers/include/clock.h"

namespace webrtc {

class FrameDecodeTiming {
 public:
  FrameDecodeTiming(Clock* clock, webrtc::VCMTiming const* timing);
  ~FrameDecodeTiming() = default;
  FrameDecodeTiming(const FrameDecodeTiming&) = delete;
  FrameDecodeTiming& operator=(const FrameDecodeTiming&) = delete;

  struct FrameSchedule {
    Timestamp max_decode_time;
    Timestamp render_time;
  };

  absl::optional<FrameSchedule> OnFrameBufferUpdated(
      uint32_t next_temporal_unit_rtp,
      uint32_t last_temporal_unit_rtp,
      bool too_many_frames_queued);

 private:
  Clock* const clock_;
  webrtc::VCMTiming const* const timing_;
};

}  // namespace webrtc

#endif  // VIDEO_FRAME_DECODE_TIMING_H_
