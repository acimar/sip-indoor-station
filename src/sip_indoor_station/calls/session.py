from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from sip_indoor_station.sip.messages import SipRequest, SipResponse

LOGGER = logging.getLogger(__name__)


@dataclass
class CallSession:
    call_id: str
    invite_request: SipRequest
    invite_source: tuple[str, int]
    invite_cseq: str
    remote_target_uri: str
    local_to_tag: str
    local_rtp_port: int
    remote_ip: str
    remote_port: int
    codec: str
    payload_type: int
    clock_rate: int = 8000
    video_offered: bool = False
    video_payload_types: list[int] | None = None
    video_codec_mappings: dict[int, str] | None = None
    video_fmtp: dict[int, str] | None = None
    sdp_answer: str = ""
    media_session: Any | None = None
    last_final_response: SipResponse | None = None
    local_cseq: int = 1
    state: str = "ringing"

    def transition(self, state: str) -> None:
        LOGGER.info("call_state call_id=%s from=%s to=%s", self.call_id, self.state, state)
        self.state = state
