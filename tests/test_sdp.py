from sip_indoor_station.sip.sdp import build_sdp_answer, parse_sdp, select_audio_codec


SDP = (
    "v=0\r\n"
    "o=- 1 1 IN IP4 192.168.1.20\r\n"
    "s=DoorStation\r\n"
    "c=IN IP4 192.168.1.20\r\n"
    "t=0 0\r\n"
    "m=audio 4002 RTP/AVP 0 8 96\r\n"
    "a=rtpmap:0 PCMU/8000\r\n"
    "a=rtpmap:8 PCMA/8000\r\n"
    "a=sendonly\r\n"
)


def test_parse_invite_sdp() -> None:
    offer = parse_sdp(SDP)
    assert offer.remote_ip == "192.168.1.20"
    assert offer.audio_port == 4002
    assert offer.payload_types == [0, 8, 96]
    assert offer.codec_mappings[8] == "PCMA"
    assert offer.direction == "sendonly"


def test_parse_invite_sdp_with_video() -> None:
    offer = parse_sdp(
        SDP
        + "m=video 4004 RTP/AVP 96 97\r\n"
        + "a=rtpmap:96 H264/90000\r\n"
        + "a=rtpmap:97 MP4V-ES/90000\r\n"
    )
    assert offer.video_offered is True
    assert offer.video_payload_types == [96, 97]


# Real Hikvision DS-KV6113 offer (captured 2026-07-22): wideband first, then G.711.
HIK_SDP = (
    "v=0\r\n"
    "o=- 1 1 IN IP4 192.168.18.30\r\n"
    "s=DoorStation\r\n"
    "c=IN IP4 192.168.18.30\r\n"
    "t=0 0\r\n"
    "m=audio 9654 RTP/AVP 115 98 0 8 103 104 100 101\r\n"
    "a=rtpmap:115 OPUS/16000\r\n"
    "a=rtpmap:98 G7221/16000\r\n"
    "a=rtpmap:0 PCMU/8000\r\n"
    "a=rtpmap:8 PCMA/8000\r\n"
    "a=rtpmap:103 G726-32/8000\r\n"
    "a=rtpmap:104 MP4A-LATM/16000\r\n"
    "a=rtpmap:100 MP4A-LATM/16000\r\n"
    "a=rtpmap:101 telephone-event/8000\r\n"
    "a=sendrecv\r\n"
)


def test_selecting_pcma_over_pcmu() -> None:
    assert select_audio_codec(parse_sdp(SDP)) == ("PCMA", 8, 8000)


def test_falling_back_to_pcmu() -> None:
    offer = parse_sdp(SDP.replace("0 8 96", "0 96").replace("a=rtpmap:8 PCMA/8000\r\n", ""))
    assert select_audio_codec(offer) == ("PCMU", 0, 8000)


def test_default_preference_ignores_offered_wideband() -> None:
    # Default preference is G.711 only → wideband PTs in the Hik offer are skipped.
    assert select_audio_codec(parse_sdp(HIK_SDP)) == ("PCMA", 8, 8000)


def test_opus_at_nonstandard_clock_is_accepted_echoing_offered_rate() -> None:
    # This doorbell offers OPUS/16000. We accept it and return the ADVERTISED
    # 16000 (echoed in the SDP answer so the device agrees); the GStreamer receive
    # pipeline separately forces 48000 in the caps (rtpopusdepay requirement).
    got = select_audio_codec(parse_sdp(HIK_SDP), preference=["OPUS", "PCMA"])
    assert got == ("OPUS", 115, 16000)


def test_opus_selected_when_offered_at_48k() -> None:
    offer = parse_sdp(HIK_SDP.replace("OPUS/16000", "OPUS/48000"))
    assert select_audio_codec(offer, preference=["OPUS", "PCMA"]) == ("OPUS", 115, 48000)


def test_available_filter_skips_unsupported_codec() -> None:
    offer = parse_sdp(HIK_SDP.replace("OPUS/16000", "OPUS/48000"))
    got = select_audio_codec(offer, preference=["OPUS", "PCMA"], available={"PCMA", "PCMU"})
    assert got == ("PCMA", 8, 8000)


def test_sdp_answer_rejects_offered_video_with_zero_port() -> None:
    answer = build_sdp_answer("192.168.1.10", 40000, "PCMA", 8, reject_video_payload_types=[96, 97])
    assert "m=audio 40000 RTP/AVP 8\r\n" in answer
    assert "m=video 0 RTP/AVP 96 97\r\n" in answer
