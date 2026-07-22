from __future__ import annotations

import secrets
from dataclasses import dataclass, field


@dataclass
class SdpOffer:
    remote_ip: str | None = None
    audio_port: int | None = None
    payload_types: list[int] = field(default_factory=list)
    video_offered: bool = False
    video_payload_types: list[int] = field(default_factory=list)
    video_codec_mappings: dict[int, str] = field(default_factory=dict)
    video_fmtp: dict[int, str] = field(default_factory=dict)
    codec_mappings: dict[int, str] = field(default_factory=dict)
    codec_clock_rates: dict[int, int] = field(default_factory=dict)
    direction: str = "sendrecv"


def parse_sdp(body: str) -> SdpOffer:
    offer = SdpOffer()
    for raw_line in body.replace("\r\n", "\n").split("\n"):
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("c="):
            parts = line[2:].split()
            if len(parts) >= 3 and parts[1].upper() == "IP4":
                offer.remote_ip = parts[2]
        elif line.startswith("m=audio"):
            parts = line[2:].split()
            if len(parts) >= 4:
                offer.audio_port = int(parts[1])
                offer.payload_types = [int(pt) for pt in parts[3:] if pt.isdigit()]
        elif line.startswith("m=video"):
            parts = line[2:].split()
            if len(parts) >= 4:
                offer.video_offered = True
                offer.video_payload_types = [int(pt) for pt in parts[3:] if pt.isdigit()]
        elif line.startswith("a=rtpmap:"):
            payload, codec = line[len("a=rtpmap:") :].split(None, 1)
            if payload.isdigit():
                payload_type = int(payload)
                codec_parts = codec.split("/")
                codec_name = codec_parts[0].upper()
                if offer.video_offered and payload_type in offer.video_payload_types:
                    offer.video_codec_mappings[payload_type] = codec
                else:
                    offer.codec_mappings[payload_type] = codec_name
                    if len(codec_parts) >= 2 and codec_parts[1].isdigit():
                        offer.codec_clock_rates[payload_type] = int(codec_parts[1])
        elif line.startswith("a=fmtp:"):
            payload, fmtp = line[len("a=fmtp:") :].split(None, 1)
            if payload.isdigit() and int(payload) in offer.video_payload_types:
                offer.video_fmtp[int(payload)] = fmtp.replace("; ", ";")
        elif line in ("a=sendrecv", "a=sendonly", "a=recvonly", "a=inactive"):
            offer.direction = line[2:]
    return offer


# Static payload types (RFC 3551) that may appear without an explicit rtpmap.
_STATIC_AUDIO_CODECS = {0: ("PCMU", 8000), 8: ("PCMA", 8000)}

# Eligibility per codec: required clock rate (None = any). OPUS in RTP is only
# standard at 48000 (RFC 7587); offers announcing other rates (e.g. Hikvision's
# OPUS/16000) do not match GStreamer's rtpopusdepay/pay templates and are skipped.
_CODEC_CLOCK_REQUIREMENTS: dict[str, int | None] = {
    "PCMA": 8000,
    "PCMU": 8000,
    "OPUS": 48000,
    "MP4A-LATM": None,
}

DEFAULT_AUDIO_CODEC_PREFERENCE = ["PCMA", "PCMU"]


def offered_audio_codecs(offer: SdpOffer) -> list[tuple[str, int, int]]:
    """All audio codecs in the offer as (name, payload_type, clock_rate), offer order."""
    result: list[tuple[str, int, int]] = []
    for payload_type in offer.payload_types:
        if payload_type in offer.codec_mappings:
            name = offer.codec_mappings[payload_type]
            clock = offer.codec_clock_rates.get(payload_type, 8000)
        elif payload_type in _STATIC_AUDIO_CODECS:
            name, clock = _STATIC_AUDIO_CODECS[payload_type]
        else:
            continue
        result.append((name, payload_type, clock))
    return result


def select_audio_codec(
    offer: SdpOffer,
    preference: list[str] | None = None,
    available: set[str] | None = None,
) -> tuple[str, int, int] | None:
    """Pick (codec, payload_type, clock_rate) honoring a preference list.

    Falls back through the preference order; a codec is eligible when offered,
    clock-rate compatible, and (when `available` is given) locally supported.
    """
    preference = [p.upper() for p in (preference or DEFAULT_AUDIO_CODEC_PREFERENCE)]
    offered = offered_audio_codecs(offer)
    for wanted in preference:
        for name, payload_type, clock in offered:
            if name != wanted:
                continue
            required_clock = _CODEC_CLOCK_REQUIREMENTS.get(name)
            if required_clock is not None and clock != required_clock:
                continue
            if available is not None and name not in available:
                continue
            return (name, payload_type, clock)
    return None


def build_sdp_answer(
    local_ip: str,
    local_port: int,
    codec: str,
    payload_type: int,
    reject_video_payload_types: list[int] | None = None,
    reject_video_codec_mappings: dict[int, str] | None = None,
    reject_video_fmtp: dict[int, str] | None = None,
    clock_rate: int = 8000,
    audio_fmtp: str | None = None,
) -> str:
    session_id = secrets.randbelow(10_000_000_000)
    lines = [
        "v=0",
        f"o=- {session_id} 1 IN IP4 {local_ip}",
        "s=SIP Indoor Station",
        f"c=IN IP4 {local_ip}",
        "t=0 0",
        f"m=audio {local_port} RTP/AVP {payload_type}",
        f"a=rtpmap:{payload_type} {codec}/{clock_rate}",
    ]
    if audio_fmtp:
        lines.append(f"a=fmtp:{payload_type} {audio_fmtp}")
    lines += [
        "a=sendrecv",
    ]
    if reject_video_payload_types is not None:
        payloads = " ".join(str(payload_type) for payload_type in reject_video_payload_types) or "0"
        lines.append(f"m=video 0 RTP/AVP {payloads}")
        for video_payload_type in reject_video_payload_types:
            codec = (reject_video_codec_mappings or {}).get(video_payload_type)
            if codec:
                lines.append(f"a=rtpmap:{video_payload_type} {codec}")
            fmtp = (reject_video_fmtp or {}).get(video_payload_type)
            if fmtp:
                lines.append(f"a=fmtp:{video_payload_type} {fmtp}")
    lines.append("")
    return "\r\n".join(lines)
