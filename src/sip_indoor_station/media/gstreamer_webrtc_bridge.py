from __future__ import annotations

import asyncio
import logging
import re
import ctypes
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from sip_indoor_station.media.base import MediaSession
from sip_indoor_station.media.gstreamer_check import check_required_elements

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class SipAudioCodecElements:
    encoding_name: str
    depay: str
    decoder: str
    encoder: str
    pay: str
    # RTP clock-rate the GStreamer elements require in caps. For Opus this is
    # always 48000 (RFC 7587 / rtpopusdepay), regardless of what the door
    # station advertises (Hikvision announces OPUS/16000) — the Opus bitstream
    # is self-describing, so forcing 48000 in the receive caps lets the packets
    # flow while opusdec still recovers the true wideband audio.
    gst_clock_rate: int = 8000


SIP_AUDIO_CODECS = {
    "PCMU": SipAudioCodecElements(
        encoding_name="PCMU",
        depay="rtppcmudepay",
        decoder="mulawdec",
        encoder="mulawenc",
        pay="rtppcmupay",
        gst_clock_rate=8000,
    ),
    "PCMA": SipAudioCodecElements(
        encoding_name="PCMA",
        depay="rtppcmadepay",
        decoder="alawdec",
        encoder="alawenc",
        pay="rtppcmapay",
        gst_clock_rate=8000,
    ),
    "OPUS": SipAudioCodecElements(
        encoding_name="OPUS",
        depay="rtpopusdepay",
        decoder="opusdec",
        encoder="opusenc",
        pay="rtpopuspay",
        gst_clock_rate=48000,
    ),
}


class GStreamerWebRtcBridge(MediaSession):
    def __init__(
        self,
        call_id: str,
        local_media_ip: str,
        local_rtp_port: int,
        remote_rtp_ip: str,
        remote_rtp_port: int,
        local_bind_ip: str = "0.0.0.0",
        selected_codec: str = "PCMU",
        selected_payload_type: int = 0,
        station_clock_rate: int | None = None,
        jitter_buffer_ms: int = 60,
        single_peer: bool = True,
        stun_servers: list[str] | None = None,
        turn_servers: list[str] | None = None,
        turn_username: str | None = None,
        turn_password: str | None = None,
        ice_transport_policy: str = "all",
        ice_candidates: list[str] | None = None,
        ice_udp_port: int | None = None,
    ) -> None:
        self.call_id = call_id
        self.local_media_ip = local_media_ip
        self.local_bind_ip = local_bind_ip
        self.local_rtp_port = local_rtp_port
        self.remote_rtp_ip = remote_rtp_ip
        self.remote_rtp_port = remote_rtp_port
        self.selected_codec = selected_codec.upper()
        self.selected_payload_type = selected_payload_type
        # Actual RTP timestamp clock the door station uses on the wire. May differ
        # from the codec's gst_clock_rate (Hikvision sends Opus with a 16000 clock
        # while GStreamer's rtpopusdepay requires 48000 caps). Defaults to the
        # codec's gst rate when unknown.
        self.station_clock_rate = station_clock_rate
        self.jitter_buffer_ms = jitter_buffer_ms
        self.single_peer = single_peer
        self.stun_servers = stun_servers or []
        self.turn_servers = turn_servers or []
        self.turn_username = turn_username
        self.turn_password = turn_password
        self.ice_transport_policy = ice_transport_policy
        self.ice_candidates = ice_candidates or []
        self.ice_udp_port = ice_udp_port
        self.pipeline: Any | None = None
        self.webrtc: Any | None = None
        self._gst: Any | None = None
        self._webrtc_api: Any | None = None
        self._sdp_api: Any | None = None
        self._glib: Any | None = None
        self._bus: Any | None = None
        self._bus_handler_id: int | None = None
        self._webrtc_handler_ids: list[int] = []
        self._main_loop: Any | None = None
        self._main_loop_thread: threading.Thread | None = None
        self._started = False
        self._prepared = False
        self._peer_active = False
        self.webrtc_opus_payload_type = 111
        self._ice_candidate_callback: Callable[[dict[str, Any]], Awaitable[None]] | None = None
        self._asyncio_loop: asyncio.AbstractEventLoop | None = None

    @property
    def sip_audio_elements(self) -> SipAudioCodecElements:
        try:
            return SIP_AUDIO_CODECS[self.selected_codec]
        except KeyError as exc:
            raise ValueError(f"unsupported SIP audio codec: {self.selected_codec}") from exc

    async def prepare(self) -> None:
        if self._prepared:
            return
        check_required_elements().assert_available()
        self._load_gstreamer()
        self._start_glib_loop()
        self.pipeline, self.webrtc = self._build_pipeline()
        if self.stun_servers:
            self.webrtc.set_property("stun-server", self.stun_servers[0])
            if len(self.stun_servers) > 1:
                LOGGER.info(
                    "gstreamer_multiple_stun_configured_using_first call_id=%s stun_server=%s extra_count=%s",
                    self.call_id,
                    self.stun_servers[0],
                    len(self.stun_servers) - 1,
                )
        for turn in self.gstreamer_turn_servers():
            self.webrtc.emit("add-turn-server", turn)
        if self.ice_transport_policy.lower() == "relay":
            self.webrtc.set_property("ice-transport-policy", self._webrtc_api.WebRTCICETransportPolicy.RELAY)
        elif self.ice_transport_policy.lower() != "all":
            LOGGER.warning(
                "unsupported_ice_transport_policy call_id=%s policy=%s",
                self.call_id,
                self.ice_transport_policy,
            )
        self._webrtc_handler_ids = [
            self.webrtc.connect("on-ice-candidate", self._on_ice_candidate),
            self.webrtc.connect("pad-added", self._on_webrtc_pad_added),
            self.webrtc.connect("notify::ice-gathering-state", self._on_ice_gathering_state),
        ]
        if hasattr(self.webrtc.props, "connection_state"):
            self._webrtc_handler_ids.append(self.webrtc.connect("notify::connection-state", self._on_connection_state))
        bus = self.pipeline.get_bus()
        bus.add_signal_watch()
        self._bus = bus
        self._bus_handler_id = bus.connect("message", self._on_bus_message)
        result = self.pipeline.set_state(self._gst.State.READY)
        if result == self._gst.StateChangeReturn.FAILURE:
            raise RuntimeError("failed to set GStreamer WebRTC bridge pipeline to READY")
        self._prepared = True
        LOGGER.info(
            "media_prepared call_id=%s advertised_rtp=%s:%s bind_rtp=%s:%s remote_rtp=%s:%s codec=%s payload=%s fixed_ice_port=%s",
            self.call_id,
            self.local_media_ip,
            self.local_rtp_port,
            self.local_bind_ip,
            self.local_rtp_port,
            self.remote_rtp_ip,
            self.remote_rtp_port,
            self.selected_codec,
            self.selected_payload_type,
            self.ice_udp_port,
        )

    async def start(self) -> None:
        await self.prepare()
        if self._started:
            return
        result = self.pipeline.set_state(self._gst.State.PLAYING)
        if result == self._gst.StateChangeReturn.FAILURE:
            raise RuntimeError("failed to set GStreamer WebRTC bridge pipeline to PLAYING")
        self._started = True
        LOGGER.info("media_started call_id=%s", self.call_id)

    async def stop(self) -> None:
        self._disconnect_gstreamer_signals()
        if self.pipeline is not None:
            self.pipeline.set_state(self._gst.State.NULL)
        if self._main_loop is not None:
            self._main_loop.quit()
        if self._main_loop_thread is not None and self._main_loop_thread.is_alive():
            self._main_loop_thread.join(timeout=2)
        self._started = False
        self._prepared = False
        self._peer_active = False
        self.pipeline = None
        self.webrtc = None
        self._bus = None
        self._main_loop = None
        self._main_loop_thread = None
        self._asyncio_loop = None
        self._ice_candidate_callback = None
        LOGGER.info("media_stopped call_id=%s", self.call_id)

    async def handle_webrtc_offer(self, sdp: str, type_: str = "offer") -> dict[str, Any]:
        if type_ != "offer":
            raise ValueError("only WebRTC offer messages are supported")
        await self.start()
        if self.single_peer and self._peer_active:
            raise RuntimeError("a WebRTC peer is already connected")
        self._asyncio_loop = asyncio.get_running_loop()
        answer = await self._run_gst_offer_answer(sdp)
        self._peer_active = True
        return {"type": "answer", "sdp": answer}

    async def add_ice_candidate(self, candidate: dict[str, Any]) -> None:
        await self.prepare()
        sdp_mline_index = int(candidate.get("sdpMLineIndex") or 0)
        candidate_text = candidate.get("candidate") or ""
        self.webrtc.emit("add-ice-candidate", sdp_mline_index, candidate_text)

    def set_ice_candidate_callback(self, callback: Callable[[dict[str, Any]], Awaitable[None]]) -> None:
        self._ice_candidate_callback = callback

    async def close_peer(self) -> None:
        self._peer_active = False

    def _load_gstreamer(self) -> None:
        if self._gst is not None:
            return
        import gi

        gi.require_version("Gst", "1.0")
        gi.require_version("GstWebRTC", "1.0")
        gi.require_version("GstSdp", "1.0")
        from gi.repository import GLib, Gst, GstSdp, GstWebRTC

        Gst.init(None)
        Gst.Plugin.load_by_name("webrtc")
        self._glib = GLib
        self._gst = Gst
        self._sdp_api = GstSdp
        self._webrtc_api = GstWebRTC

    def _start_glib_loop(self) -> None:
        if self._main_loop is not None:
            return
        self._main_loop = self._glib.MainLoop()
        self._main_loop_thread = threading.Thread(
            target=self._main_loop.run,
            name=f"gst-webrtc-{self.call_id}",
            daemon=True,
        )
        self._main_loop_thread.start()

    def _build_pipeline(self) -> tuple[Any, Any]:
        self._validate_ice_udp_port()
        pipeline = self._gst.parse_launch(self._pipeline_description())
        webrtc = pipeline.get_by_name("webrtc")
        if webrtc is None:
            raise RuntimeError("GStreamer pipeline did not create webrtcbin")
        webrtc.set_property("bundle-policy", self._webrtc_api.WebRTCBundlePolicy.MAX_BUNDLE)

        if self._needs_retimestamp:
            self._install_retimestamp_probe(pipeline)

        if self.ice_udp_port is not None:
            ice_agent = webrtc.get_property("ice-agent")

            if ice_agent is None:
                raise RuntimeError("GStreamer webrtcbin did not expose an ICE agent")

            # Ugly workaround for bug described here:
            # https://gitlab.gnome.org/GNOME/pygobject/-/work_items/605
            py_object = ctypes.py_object(ice_agent)
            ctypes.pythonapi.Py_IncRef(py_object)

            ice_agent.set_property("min-rtp-port", self.ice_udp_port)
            ice_agent.set_property("max-rtp-port", self.ice_udp_port)
            ice_agent.set_property("ice-tcp", False)

            LOGGER.info(
                "webrtc_ice_agent_configured call_id=%s fixed_ice_port=%s min_rtp_port=%s max_rtp_port=%s ice_udp=%s ice_tcp=%s",
                self.call_id,
                self.ice_udp_port,
                ice_agent.get_property("min-rtp-port"),
                ice_agent.get_property("max-rtp-port"),
                ice_agent.get_property("ice-udp"),
                ice_agent.get_property("ice-tcp"),
            )
        return pipeline, webrtc

    def _disconnect_gstreamer_signals(self) -> None:
        if self.webrtc is not None:
            for handler_id in self._webrtc_handler_ids:
                try:
                    self.webrtc.disconnect(handler_id)
                except Exception:
                    LOGGER.debug("webrtc_signal_disconnect_failed call_id=%s handler_id=%s", self.call_id, handler_id, exc_info=True)
        self._webrtc_handler_ids = []

        if self._bus is not None:
            if self._bus_handler_id is not None:
                try:
                    self._bus.disconnect(self._bus_handler_id)
                except Exception:
                    LOGGER.debug("bus_signal_disconnect_failed call_id=%s handler_id=%s", self.call_id, self._bus_handler_id, exc_info=True)
            self._bus_handler_id = None
            try:
                self._bus.remove_signal_watch()
            except Exception:
                LOGGER.debug("bus_signal_watch_remove_failed call_id=%s", self.call_id, exc_info=True)

    def _validate_ice_udp_port(self) -> None:
        if self.ice_udp_port is not None and (self.ice_udp_port < 1 or self.ice_udp_port > 65535):
            raise ValueError(f"invalid WebRTC ICE UDP port: {self.ice_udp_port}")

    @property
    def _sip_raw_rate(self) -> int:
        """Raw-audio sample rate on the SIP side of the bridge.

        G.711 is 8 kHz; Opus works natively at 48 kHz (matches the browser leg,
        avoiding an extra resample). Used for both decode-out and encode-in caps.
        """
        return 48000 if self.selected_codec == "OPUS" else 8000

    @property
    def _needs_retimestamp(self) -> bool:
        """True when the station's RTP clock differs from the depayloader's.

        rtpopusdepay mandates a 48000 caps clock, but some door stations stamp
        Opus at a non-standard rate (Hikvision advertises 16000 and stamps as if
        8000: 320 units per 40 ms frame). Interpreting those timestamps at 48000
        compresses PTS ~6x, so the browser plays chopped audio. When this holds,
        we rewrite PTS after the decoder from the accumulated buffer durations
        (which the decoder reports correctly) — see _install_retimestamp_probe.
        """
        return bool(self.station_clock_rate) and self.station_clock_rate != self.sip_audio_elements.gst_clock_rate

    def _pipeline_description(self) -> str:
        codec = self.sip_audio_elements
        # udpsrc advertises the codec's GStreamer clock (48000 for Opus — what
        # rtpopusdepay mandates) so the whole receive branch prerolls and WebRTC
        # reaches PLAYING. The decoder is named so a pad probe can fix PTS when
        # the station's RTP clock is non-standard (see _needs_retimestamp).
        return (
            f"udpsrc name=station_rtp_src port={self.local_rtp_port} "
            f'caps="application/x-rtp,media=audio,clock-rate={codec.gst_clock_rate},encoding-name={codec.encoding_name},payload={self.selected_payload_type}" '
            f"! queue ! rtpjitterbuffer latency={self.jitter_buffer_ms} "
            f"! {codec.depay} ! {codec.decoder} name=sip_decoder ! audioconvert ! audioresample "
            "! audio/x-raw,rate=48000,channels=1 "
            # Browser-facing Opus tuned for lossy links (WiFi/4G): in-band FEC lets
            # the browser reconstruct dropped packets, packet-loss-percentage sizes
            # that redundancy, audio-type=voice optimizes speech. This is where a
            # SIP bridge earns "answer anywhere with good quality".
            f"! queue ! opusenc audio-type=voice inband-fec=true packet-loss-percentage=15 "
            f"! rtpopuspay name=browser_audio_pay pt={self.webrtc_opus_payload_type} "
            f'! capsfilter name=browser_audio_caps caps="application/x-rtp,media=audio,clock-rate=48000,encoding-name=OPUS,payload={self.webrtc_opus_payload_type}" '
            "! queue ! webrtcbin name=webrtc"
        )

    def _install_retimestamp_probe(self, pipeline: Any) -> None:
        """Rewrite decoded-audio PTS from accumulated buffer durations.

        Neutralises the ~6x PTS compression caused by a non-standard station RTP
        clock (see _needs_retimestamp). The decoder sets each buffer's duration
        correctly (e.g. 40 ms per Opus frame), so a monotonic PTS built from those
        durations restores real-time playback. Validated offline against real
        Hikvision Opus RTP: 6.7 ms -> 40.0 ms median PTS delta.
        """
        decoder = pipeline.get_by_name("sip_decoder")
        if decoder is None:
            LOGGER.warning("retimestamp_probe_no_decoder call_id=%s", self.call_id)
            return
        src_pad = decoder.get_static_pad("src")
        if src_pad is None:
            LOGGER.warning("retimestamp_probe_no_src_pad call_id=%s", self.call_id)
            return
        acc = {"pts": 0}
        gst = self._gst

        def _retime(_pad: Any, info: Any) -> Any:
            try:
                buf = info.get_buffer()
                buf.pts = acc["pts"]
                buf.dts = acc["pts"]
                duration = buf.duration
                if duration and duration != gst.CLOCK_TIME_NONE:
                    acc["pts"] += duration
            except Exception:
                LOGGER.debug("retimestamp_probe_failed call_id=%s", self.call_id, exc_info=True)
            return gst.PadProbeReturn.OK

        src_pad.add_probe(gst.PadProbeType.BUFFER, _retime)
        LOGGER.info(
            "retimestamp_probe_installed call_id=%s station_clock=%s depay_clock=%s",
            self.call_id,
            self.station_clock_rate,
            self.sip_audio_elements.gst_clock_rate,
        )

    async def _run_gst_offer_answer(self, sdp: str) -> str:
        future: asyncio.Future[str] = asyncio.get_running_loop().create_future()

        def run() -> bool:
            try:
                self.configure_browser_opus_payload_type(sdp)
                result, msg = self._sdp_api.SDPMessage.new()
                if result != self._sdp_api.SDPResult.OK:
                    raise RuntimeError("failed to allocate SDP message")
                result = self._sdp_api.sdp_message_parse_buffer(bytes(sdp.encode()), msg)
                if result != self._sdp_api.SDPResult.OK:
                    raise RuntimeError("failed to parse browser SDP offer")
                offer = self._webrtc_api.WebRTCSessionDescription.new(
                    self._webrtc_api.WebRTCSDPType.OFFER,
                    msg,
                )

                def on_answer_created(promise: Any, _userdata: Any) -> None:
                    try:
                        reply = promise.get_reply()
                        answer = reply.get_value("answer")
                        self.webrtc.emit("set-local-description", answer, self._gst.Promise.new())
                        self._asyncio_loop.call_soon_threadsafe(future.set_result, answer.sdp.as_text())
                    except Exception as exc:
                        self._asyncio_loop.call_soon_threadsafe(future.set_exception, exc)

                def on_remote_set(_promise: Any, _userdata: Any) -> None:
                    promise = self._gst.Promise.new_with_change_func(on_answer_created, None)
                    self.webrtc.emit("create-answer", None, promise)

                promise = self._gst.Promise.new_with_change_func(on_remote_set, None)
                self.webrtc.emit("set-remote-description", offer, promise)
            except Exception as exc:
                self._asyncio_loop.call_soon_threadsafe(future.set_exception, exc)
            return False

        self._glib.idle_add(run)
        return await future

    def configure_browser_opus_payload_type(self, sdp: str) -> None:
        payload_type = self.find_opus_payload_type(sdp)
        if payload_type is None:
            LOGGER.warning("webrtc_offer_missing_opus call_id=%s using_default_payload=%s", self.call_id, self.webrtc_opus_payload_type)
            return
        self.webrtc_opus_payload_type = payload_type
        pay = self.pipeline.get_by_name("browser_audio_pay") if self.pipeline is not None else None
        capsfilter = self.pipeline.get_by_name("browser_audio_caps") if self.pipeline is not None else None
        if pay is not None:
            pay.set_property("pt", payload_type)
        if capsfilter is not None:
            capsfilter.set_property(
                "caps",
                self._gst.Caps.from_string(
                    f"application/x-rtp,media=audio,clock-rate=48000,encoding-name=OPUS,payload={payload_type}"
                ),
            )
        LOGGER.info("webrtc_opus_payload_selected call_id=%s payload_type=%s", self.call_id, payload_type)

    @staticmethod
    def find_opus_payload_type(sdp: str) -> int | None:
        audio_payloads: set[int] = set()
        in_audio = False
        for raw_line in sdp.replace("\r\n", "\n").split("\n"):
            line = raw_line.strip()
            if line.startswith("m="):
                in_audio = line.startswith("m=audio")
                if in_audio:
                    audio_payloads = {int(part) for part in line.split()[3:] if part.isdigit()}
                continue
            if in_audio and line.startswith("a=rtpmap:"):
                payload, codec = line[len("a=rtpmap:") :].split(None, 1)
                if payload.isdigit() and int(payload) in audio_payloads and codec.upper().startswith("OPUS/"):
                    return int(payload)
        return None

    def _on_ice_candidate(self, _webrtc: Any, mline_index: int, candidate: str) -> None:
        LOGGER.debug("webrtc_local_ice call_id=%s mline=%s", self.call_id, mline_index)
        if self._ice_candidate_callback is None or self._asyncio_loop is None:
            return
        for candidate_text in self.local_ice_candidates(candidate):
            payload = {"candidate": candidate_text, "sdpMid": str(mline_index), "sdpMLineIndex": int(mline_index)}
            asyncio.run_coroutine_threadsafe(self._ice_candidate_callback(payload), self._asyncio_loop)

    def gstreamer_turn_servers(self) -> list[str]:
        return [self.format_gstreamer_turn_server(server) for server in self.turn_servers]

    def gstreamer_turn_server(self) -> str:
        return self.gstreamer_turn_servers()[0] if self.turn_servers else ""

    def format_gstreamer_turn_server(self, server: str) -> str:
        if server.startswith("turn:") and not server.startswith("turn://"):
            server = "turn://" + server[len("turn:") :]
        elif server.startswith("turns:") and not server.startswith("turns://"):
            server = "turns://" + server[len("turns:") :]
        if not self.turn_username or not self.turn_password:
            return server
        match = re.match(r"^(turns?://)(.+)$", server)
        if match:
            return f"{match.group(1)}{self.turn_username}:{self.turn_password}@{match.group(2)}"
        return f"turn://{self.turn_username}:{self.turn_password}@{server}"

    def local_ice_candidates(self, candidate: str) -> list[str]:
        if not self.ice_candidates or not self.is_host_ice_candidate(candidate):
            return [candidate]
        configured = [self.ice_candidate_for_configured_host(candidate, ice_candidate) for ice_candidate in self.ice_candidates]
        return [*configured, candidate]

    def ice_candidate_for_first_configured_host(self, candidate: str) -> str:
        if not self.ice_candidates:
            return candidate
        return self.ice_candidate_for_configured_host(candidate, self.ice_candidates[0])

    @classmethod
    def ice_candidate_for_configured_host(cls, candidate: str, configured_host: str) -> str:
        parts = candidate.split()
        if len(parts) >= 8 and parts[7] == "host":
            host, port = cls.parse_configured_ice_candidate(configured_host)
            parts[4] = host
            if port is not None:
                parts[5] = str(port)
            return " ".join(parts)
        return candidate

    @staticmethod
    def parse_configured_ice_candidate(configured_host: str) -> tuple[str, int | None]:
        host, separator, port_text = configured_host.rpartition(":")
        if separator and port_text.isdigit():
            return host, int(port_text)
        return configured_host, None

    @staticmethod
    def is_host_ice_candidate(candidate: str) -> bool:
        parts = candidate.split()
        return len(parts) >= 8 and parts[7] == "host"

    def _on_webrtc_pad_added(self, _webrtc: Any, pad: Any) -> None:
        caps = pad.get_current_caps() or pad.query_caps(None)
        caps_text = caps.to_string() if caps is not None else ""
        if "application/x-rtp" not in caps_text or "media=(string)audio" not in caps_text:
            LOGGER.debug("webrtc_pad_ignored call_id=%s caps=%s", self.call_id, caps_text)
            return
        LOGGER.info("webrtc_audio_pad_added call_id=%s", self.call_id)
        codec = self.sip_audio_elements
        depay = self._gst.ElementFactory.make("rtpopusdepay")
        dec = self._gst.ElementFactory.make("opusdec")
        convert = self._gst.ElementFactory.make("audioconvert")
        resample = self._gst.ElementFactory.make("audioresample")
        capsfilter = self._gst.ElementFactory.make("capsfilter")
        capsfilter.set_property(
            "caps",
            self._gst.Caps.from_string(f"audio/x-raw,format=S16LE,rate={self._sip_raw_rate},channels=1"),
        )
        enc = self._gst.ElementFactory.make(codec.encoder)
        pay = self._gst.ElementFactory.make(codec.pay)
        pay.set_property("pt", int(self.selected_payload_type))
        if codec.encoding_name == "OPUS":
            # Match the door station's advertised (non-standard) 16 kHz Opus: encode
            # narrower audio so the device's decoder timing stays sane. opusenc keeps
            # the bitstream self-describing, so this only bounds the bandwidth.
            enc.set_property("audio-type", "voice")
        sink = self._gst.ElementFactory.make("udpsink")
        self._configure_station_udp_sink(sink)
        queue = self._gst.ElementFactory.make("queue")
        elements = [queue, depay, dec, convert, resample, capsfilter, enc, pay, sink]
        for element in elements:
            self.pipeline.add(element)
            element.sync_state_with_parent()
        for left, right in zip(elements, elements[1:]):
            if not left.link(right):
                LOGGER.error("webrtc_audio_link_failed call_id=%s left=%s right=%s", self.call_id, left.name, right.name)
                return
        sinkpad = queue.get_static_pad("sink")
        if pad.link(sinkpad) != self._gst.PadLinkReturn.OK:
            LOGGER.error("webrtc_audio_pad_link_failed call_id=%s", self.call_id)

    def _configure_station_udp_sink(self, sink: Any) -> None:
        sink.set_property("host", self.remote_rtp_ip)
        sink.set_property("port", int(self.remote_rtp_port))
        sink.set_property("sync", False)
        sink.set_property("async", False)

        station_src = self.pipeline.get_by_name("station_rtp_src") if self.pipeline is not None else None
        used_socket = station_src.get_property("used-socket") if station_src is not None else None
        if used_socket is not None:
            sink.set_property("socket", used_socket)
            sink.set_property("close-socket", False)
            LOGGER.info(
                "station_rtp_out_shared_socket call_id=%s advertised_rtp=%s:%s bind_rtp=%s:%s remote_rtp=%s:%s",
                self.call_id,
                self.local_media_ip,
                self.local_rtp_port,
                self.local_bind_ip,
                self.local_rtp_port,
                self.remote_rtp_ip,
                self.remote_rtp_port,
            )
            return

        sink.set_property("bind-address", self.local_bind_ip)
        sink.set_property("bind-port", int(self.local_rtp_port))
        LOGGER.info(
            "station_rtp_out_bound_port call_id=%s advertised_rtp=%s:%s bind_rtp=%s:%s remote_rtp=%s:%s",
            self.call_id,
            self.local_media_ip,
            self.local_rtp_port,
            self.local_bind_ip,
            self.local_rtp_port,
            self.remote_rtp_ip,
            self.remote_rtp_port,
        )

    def _on_ice_gathering_state(self, webrtc: Any, _param: Any) -> None:
        LOGGER.debug("webrtc_ice_gathering call_id=%s state=%s", self.call_id, webrtc.get_property("ice-gathering-state"))

    def _on_connection_state(self, webrtc: Any, _param: Any) -> None:
        LOGGER.info("webrtc_connection_state call_id=%s state=%s", self.call_id, webrtc.get_property("connection-state"))

    def _on_bus_message(self, _bus: Any, message: Any) -> None:
        msg_type = message.type
        if msg_type == self._gst.MessageType.ERROR:
            err, debug = message.parse_error()
            LOGGER.error("gstreamer_error call_id=%s error=%s debug=%s", self.call_id, err, debug)
        elif msg_type == self._gst.MessageType.WARNING:
            err, debug = message.parse_warning()
            LOGGER.warning("gstreamer_warning call_id=%s warning=%s debug=%s", self.call_id, err, debug)
        elif msg_type == self._gst.MessageType.EOS:
            LOGGER.info("gstreamer_eos call_id=%s", self.call_id)
        elif msg_type == self._gst.MessageType.STATE_CHANGED and message.src == self.pipeline:
            old, new, pending = message.parse_state_changed()
            LOGGER.debug("gstreamer_state call_id=%s old=%s new=%s pending=%s", self.call_id, old, new, pending)
