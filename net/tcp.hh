/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef TCP_HH_
#define TCP_HH_

#include "core/shared_ptr.hh"
#include "core/queue.hh"
#include "core/semaphore.hh"
#include "net.hh"
#include "ip_checksum.hh"
#include "ip.hh"
#include "const.hh"
#include <unordered_map>
#include <map>
#include <functional>
#include <deque>
#include <chrono>
#include <experimental/optional>

using namespace std::chrono_literals;

namespace net {

class tcp_hdr;

struct tcp_option {
    // The kind and len field are fixed and defined in TCP protocol
    enum class option_kind: uint8_t { mss = 2, win_scale = 3, sack = 4, timestamps = 8,  nop = 1, eol = 0 };
    enum class option_len:  uint8_t { mss = 4, win_scale = 3, sack = 2, timestamps = 10, nop = 1, eol = 1 };
    struct mss {
        option_kind kind = option_kind::mss;
        option_len len = option_len::mss;
        packed<uint16_t> mss;
        template <typename Adjuster>
        void adjust_endianness(Adjuster a) { a(mss); }
    } __attribute__((packed));
    struct win_scale {
        option_kind kind = option_kind::win_scale;
        option_len len = option_len::win_scale;
        uint8_t shift;
    } __attribute__((packed));
    struct sack {
        option_kind kind = option_kind::sack;
        option_len len = option_len::sack;
    } __attribute__((packed));
    struct timestamps {
        option_kind kind = option_kind::timestamps;
        option_len len = option_len::timestamps;
        packed<uint32_t> t1;
        packed<uint32_t> t2;
        template <typename Adjuster>
        void adjust_endianness(Adjuster a) { a(t1, t2); }
    } __attribute__((packed));
    struct nop {
        option_kind kind = option_kind::nop;
    } __attribute__((packed));
    struct eol {
        option_kind kind = option_kind::eol;
    } __attribute__((packed));
    static const uint8_t align = 4;

    void parse(uint8_t* beg, uint8_t* end);
    uint8_t fill(tcp_hdr* th, uint8_t option_size);
    uint8_t get_size();

    // For option negotiattion
    bool _mss_received = false;
    bool _win_scale_received = false;
    bool _timestamps_received = false;
    bool _sack_received = false;

    // Option data
    uint16_t _remote_mss = 536;
    uint16_t _local_mss;
    uint8_t _remote_win_scale = 0;
    uint8_t _local_win_scale = 0;
};
inline uint8_t*& operator+=(uint8_t*& x, tcp_option::option_len len) { x += uint8_t(len); return x; }
inline uint8_t& operator+=(uint8_t& x, tcp_option::option_len len) { x += uint8_t(len); return x; }

struct tcp_seq {
    uint32_t raw;
};

inline tcp_seq ntoh(tcp_seq s) {
    return tcp_seq { ntoh(s.raw) };
}

inline tcp_seq hton(tcp_seq s) {
    return tcp_seq { hton(s.raw) };
}

inline
std::ostream& operator<<(std::ostream& os, tcp_seq s) {
    return os << s.raw;
}

inline tcp_seq make_seq(uint32_t raw) { return tcp_seq{raw}; }
inline tcp_seq& operator+=(tcp_seq& s, int32_t n) { s.raw += n; return s; }
inline tcp_seq& operator-=(tcp_seq& s, int32_t n) { s.raw -= n; return s; }
inline tcp_seq operator+(tcp_seq s, int32_t n) { return s += n; }
inline tcp_seq operator-(tcp_seq s, int32_t n) { return s -= n; }
inline int32_t operator-(tcp_seq s, tcp_seq q) { return s.raw - q.raw; }
inline bool operator==(tcp_seq s, tcp_seq q)  { return s.raw == q.raw; }
inline bool operator!=(tcp_seq s, tcp_seq q) { return !(s == q); }
inline bool operator<(tcp_seq s, tcp_seq q) { return s - q < 0; }
inline bool operator>(tcp_seq s, tcp_seq q) { return q < s; }
inline bool operator<=(tcp_seq s, tcp_seq q) { return !(s > q); }
inline bool operator>=(tcp_seq s, tcp_seq q) { return !(s < q); }

inline tcp_seq get_tcp_isn() {
    // FIXME: should increase every 4ms
    return make_seq(1000000);
}

struct tcp_hdr {
    packed<uint16_t> src_port;
    packed<uint16_t> dst_port;
    packed<tcp_seq> seq;
    packed<tcp_seq> ack;
    uint8_t rsvd1 : 4;
    uint8_t data_offset : 4;
    uint8_t f_fin : 1;
    uint8_t f_syn : 1;
    uint8_t f_rst : 1;
    uint8_t f_psh : 1;
    uint8_t f_ack : 1;
    uint8_t f_urg : 1;
    uint8_t rsvd2 : 2;
    packed<uint16_t> window;
    packed<uint16_t> checksum;
    packed<uint16_t> urgent;

    template <typename Adjuster>
    void adjust_endianness(Adjuster a) { a(src_port, dst_port, seq, ack, window, checksum, urgent); }
} __attribute__((packed));

template <typename InetTraits>
class tcp {
public:
    using ipaddr = typename InetTraits::address_type;
    using inet_type = typename InetTraits::inet_type;
    using connid = l4connid<InetTraits>;
    using connid_hash = typename connid::connid_hash;
    class connection;
    class listener;
private:
    class tcb;

    class tcb {
        using clock_type = std::chrono::high_resolution_clock;
        // Instead of tracking state through an enum, track individual
        // bits of the state.  This reduces duplication in state handling.
        bool _local_syn_sent = false;
        bool _local_syn_acked = false;
        bool _foreign_syn_received = false;
        bool _local_fin_sent = false;
        bool _local_fin_acked = false;
        bool _foreign_fin_received = false;
        tcp& _tcp;
        connection* _conn = nullptr;
        ipaddr _local_ip;
        ipaddr _foreign_ip;
        uint16_t _local_port;
        uint16_t _foreign_port;
        struct unacked_segment {
            packet p;
            uint16_t data_len;
            unsigned nr_transmits;
        };
        struct send {
            tcp_seq unacknowledged;
            tcp_seq next;
            uint32_t window;
            uint8_t window_scale;
            uint16_t mss;
            tcp_seq urgent;
            tcp_seq wl1;
            tcp_seq wl2;
            tcp_seq initial;
            std::deque<unacked_segment> data;
            std::deque<packet> unsent;
            uint32_t unsent_len = 0;
            bool closed = false;
            promise<> _window_opened;
            // Wait for all data are acked
            std::experimental::optional<promise<>> _all_data_acked_promise;
            // Limit number of data queued into send queue
            semaphore user_queue_space = {212992};
        } _snd;
        struct receive {
            tcp_seq next;
            uint32_t window;
            uint8_t window_scale;
            uint16_t mss;
            tcp_seq urgent;
            tcp_seq initial;
            std::deque<packet> data;
            std::map<tcp_seq, packet> out_of_order;
            std::experimental::optional<promise<>> _data_received_promise;
        } _rcv;
        tcp_option _option;
        timer _delayed_ack;
        // RTO: retransmission timeout
        std::chrono::milliseconds _retransmit_timeout{3000};
        static constexpr uint16_t _max_nr_retransmit{5};
        timer _retransmit;
    public:
        tcb(tcp& t, connid id);
        void input(tcp_hdr* th, packet p);
        void output();
        future<> wait_for_data();
        future<> wait_for_all_data_acked();
        future<> send(packet p);
        packet read();
        void close();
        void remove_from_tcbs() {
            auto id = connid{_local_ip, _foreign_ip, _local_port, _foreign_port};
            _tcp._tcbs.erase(id);
        }
        bool both_closed() { return _foreign_fin_received && _local_fin_acked; }
    private:
        void respond_with_reset(tcp_hdr* th);
        void merge_out_of_order();
        void insert_out_of_order(tcp_seq seq, packet p);
        void trim_receive_data_after_window();
        bool should_send_ack();
        void clear_delayed_ack();
        packet get_transmit_packet();
        void start_retransmit_timer() { _retransmit.rearm(clock_type::now() + _retransmit_timeout); };
        void stop_retransmit_timer() { _retransmit.cancel(); };
        void retransmit();
        void cleanup();
        uint32_t can_send() {
           return std::min(uint32_t(_snd.unacknowledged + _snd.window - _snd.next), _snd.unsent_len);
        }
        friend class connection;
    };
    inet_type& _inet;
    std::unordered_map<connid, shared_ptr<tcb>, connid_hash> _tcbs;
    std::unordered_map<uint16_t, listener*> _listening;
public:
    class connection {
        shared_ptr<tcb> _tcb;
    public:
        explicit connection(shared_ptr<tcb> tcbp) : _tcb(std::move(tcbp)) { _tcb->_conn = this; }
        connection(const connection&) = delete;
        connection(connection&& x) noexcept : _tcb(std::move(x._tcb)) {
            _tcb->_conn = this;
        }
        ~connection();
        void operator=(const connection&) = delete;
        connection& operator=(connection&& x) {
            if (this != &x) {
                this->~connection();
                new (this) connection(std::move(x));
            }
            return *this;
        }
        future<> send(packet p) {
            return _tcb->send(std::move(p));
        }
        future<> wait_for_data() {
            return _tcb->wait_for_data();
        }
        packet read() {
            return _tcb->read();
        }
        void close_read();
        void close_write();
    };
    class listener {
        tcp& _tcp;
        uint16_t _port;
        queue<connection> _q;
    private:
        listener(tcp& t, uint16_t port, size_t queue_length)
            : _tcp(t), _port(port), _q(queue_length) {
            _tcp._listening.emplace(_port, this);
        }
    public:
        listener(listener&& x)
            : _tcp(x._tcp), _port(x._port), _q(std::move(x._q)) {
            _tcp._listening[_port] = this;
            _port = 0;
        }
        ~listener() {
            if (_port) {
                _tcp._listening.erase(_port);
            }
        }
        future<connection> accept() {
            return _q.not_empty().then([this] {
                return make_ready_future<connection>(_q.pop());
            });
        }
        friend class tcp;
    };
public:
    explicit tcp(inet_type& inet) : _inet(inet) {}
    void received(packet p, ipaddr from, ipaddr to);
    unsigned forward(packet& p, size_t off, ipaddr from, ipaddr to);
    listener listen(uint16_t port, size_t queue_length = 100);
    net::hw_features hw_features() { return _inet._inet.hw_features(); }
private:
    void send(ipaddr from, ipaddr to, packet p);
    void respond_with_reset(tcp_hdr* rth, ipaddr local_ip, ipaddr foreign_ip);
    friend class listener;
};

template <typename InetTraits>
auto tcp<InetTraits>::listen(uint16_t port, size_t queue_length) -> listener {
    return listener(*this, port, queue_length);
}

template <typename InetTraits>
unsigned tcp<InetTraits>::forward(packet& p, size_t off, ipaddr from, ipaddr to) {
    auto th = p.get_header<tcp_hdr>(off);
    if (!th) {
        return engine.cpu_id();
    }
    auto dst = ntohs(uint16_t(th->dst_port));
    auto src = ntohs(uint16_t(th->src_port));
    return connid_hash()(connid{to, from, dst, src}) % smp::count;
}

template <typename InetTraits>
void tcp<InetTraits>::received(packet p, ipaddr from, ipaddr to) {
    auto th = p.get_header<tcp_hdr>(0);
    if (!th) {
        return;
    }
    // th->data_offset is correct even before ntoh()
    if (unsigned(th->data_offset * 4) < sizeof(*th)) {
        return;
    }

    if (!hw_features().rx_csum_offload) {
        checksummer csum;
        InetTraits::tcp_pseudo_header_checksum(csum, from, to, p.len());
        csum.sum(p);
        if (csum.get() != 0) {
            return;
        }
    }
    auto h = ntoh(*th);
    auto id = connid{to, from, h.dst_port, h.src_port};
    auto tcbi = _tcbs.find(id);
    shared_ptr<tcb> tcbp;
    if (tcbi == _tcbs.end()) {
        if (h.f_syn && !h.f_ack) {
            auto listener = _listening.find(id.local_port);
            if (listener == _listening.end() || listener->second->_q.full()) {
                return respond_with_reset(&h, id.local_ip, id.foreign_ip);
            }
            tcbp = make_shared<tcb>(*this, id);
            listener->second->_q.push(connection(tcbp));
            _tcbs.insert({id, tcbp});
        }
    } else {
        tcbp = tcbi->second;
    }
    if (tcbp) {
        tcbp->input(&h, std::move(p));
    } else {
        respond_with_reset(&h, id.local_ip, id.foreign_ip);
    }
}

template <typename InetTraits>
void tcp<InetTraits>::send(ipaddr from, ipaddr to, packet p) {
    _inet.send(from, to, std::move(p));
}

template <typename InetTraits>
tcp<InetTraits>::connection::~connection() {
    if (_tcb) {
        _tcb->_conn = nullptr;
        close_read();
        close_write();
    }
}

template <typename InetTraits>
tcp<InetTraits>::tcb::tcb(tcp& t, connid id)
    : _tcp(t)
    , _local_ip(id.local_ip)
    , _foreign_ip(id.foreign_ip)
    , _local_port(id.local_port)
    , _foreign_port(id.foreign_port) {
        _delayed_ack.set_callback([this] { output(); });
        _retransmit.set_callback([this] { retransmit(); });
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::respond_with_reset(tcp_hdr* rth) {
    _tcp.respond_with_reset(rth, _local_ip, _foreign_ip);
}

template <typename InetTraits>
void tcp<InetTraits>::respond_with_reset(tcp_hdr* rth, ipaddr local_ip, ipaddr foreign_ip) {
    if (rth->f_rst) {
        return;
    }
    packet p;
    auto th = p.prepend_header<tcp_hdr>();
    th->src_port = rth->dst_port;
    th->dst_port = rth->src_port;
    if (rth->f_ack) {
        th->seq = rth->ack;
    }
    // If this RST packet is in response to a SYN packet. We ACK the ISN.
    if (rth->f_syn) {
        th->ack = rth->seq + 1;
        th->f_ack = true;
    }
    th->f_rst = true;
    th->data_offset = sizeof(*th) / 4;
    th->checksum = 0;
    *th = hton(*th);

    checksummer csum;
    InetTraits::tcp_pseudo_header_checksum(csum, local_ip, foreign_ip, sizeof(*th));
    if (hw_features().tx_csum_offload) {
        th->checksum = ~csum.get();
    } else {
        csum.sum(p);
        th->checksum = csum.get();
    }

    offload_info oi;
    oi.protocol = ip_protocol_num::tcp;
    oi.tcp_hdr_len = sizeof(tcp_hdr);
    p.set_offload_info(oi);

    send(local_ip, foreign_ip, std::move(p));
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::input(tcp_hdr* th, packet p) {
    auto opt_start = p.get_header<uint8_t>(sizeof(tcp_hdr));
    auto opt_end = opt_start + th->data_offset * 4;

    p.trim_front(th->data_offset * 4);

    bool do_output = false;
    bool do_output_data = false;
    tcp_seq seg_seq = th->seq;
    auto seg_len = p.len();

    if (th->f_rst) {
        cleanup();
        return;
    }
    if (th->f_syn) {
        if (!_foreign_syn_received) {
            _foreign_syn_received = true;
            _rcv.initial = seg_seq;
            _rcv.next = _rcv.initial + 1;
            _rcv.urgent = _rcv.next;
            _snd.wl1 = th->seq;
            _snd.next = _snd.initial = get_tcp_isn();
            _option.parse(opt_start, opt_end);
            // Remote receive window scale factor
            _snd.window_scale = _option._remote_win_scale;
            // Local receive window scale factor
            _rcv.window_scale = _option._local_win_scale;
            // Maximum segment size remote can receive
            _snd.mss = _option._remote_mss;
            // Maximum segment size local can receive
            _rcv.mss = _option._local_mss =
                _tcp.hw_features().mtu - sizeof(tcp_hdr) - net::ip_hdr_len_min;
            // Linux's default window size
            _rcv.window = 29200 << _rcv.window_scale;

            // Send <SYN,ACK> back
            do_output = true;
        } else {
            if (seg_seq != _rcv.initial) {
                return respond_with_reset(th);
            }
        }
    } else {
        // data segment
        if (seg_len
                && (seg_seq >= _rcv.next || seg_seq + seg_len <= _rcv.next + _rcv.window)) {
            // FIXME: handle urgent data (not urgent)
            if (seg_seq < _rcv.next) {
                // ignore already acknowledged data
                auto dup = std::min(uint32_t(_rcv.next - seg_seq), seg_len);
                p.trim_front(dup);
                seg_len -= dup;
                seg_seq += dup;
            }
            if (seg_len) {
                if (seg_seq == _rcv.next) {
                    _rcv.data.push_back(std::move(p));
                    _rcv.next += seg_len;
                    merge_out_of_order();
                    if (_rcv._data_received_promise) {
                        _rcv._data_received_promise->set_value();
                        _rcv._data_received_promise = {};
                    }
                } else {
                    insert_out_of_order(seg_seq, std::move(p));
                }
            }
            // Send <ACK> to ack data only when data is present in this packet
            do_output = should_send_ack();
        }
    }
    if (th->f_fin) {
        if (!_local_syn_acked) {
            return respond_with_reset(th);
        }
        auto fin_seq = seg_seq + seg_len;
        if (!_foreign_fin_received) {
            if (fin_seq < _rcv.next || fin_seq > _rcv.next + _rcv.window) {
                // FIXME: we did queue the data.  Do we care?
                return respond_with_reset(th);
            } else if (fin_seq == _rcv.next) {
                // FIXME: we might queue an out-of-order FIN.  Is it worthwhile?
                _foreign_fin_received = true;
                _rcv.next = fin_seq + 1;
                if (_rcv._data_received_promise) {
                    _rcv._data_received_promise->set_value();
                    _rcv._data_received_promise = {};
                }
                // If this <FIN> packet contains data as well, we can ACK both data
                // and <FIN> in a single packet, so canncel the previous ACK.
                clear_delayed_ack();
                do_output = false;
                output();

                // FIXME: Implement TIME-WAIT state
                if (both_closed()) {
                    cleanup();
                    return;
                }
            }
        } else {
            if (fin_seq + 1 != _rcv.next) {
                return;
            }
        }
    }
    if (th->f_ack) {
        if (!_local_syn_sent) {
            return; // FIXME: reset too?
        }
        if (!_local_syn_acked) {
            if (th->ack == _snd.initial + 1) {
                _snd.unacknowledged = _snd.next = _snd.initial + 1;
                _local_syn_acked = true;
                _snd.wl2 = th->ack;
            } else {
                return respond_with_reset(th);
            }
        }
        auto data_ack = th->ack - th->f_fin;
        if (data_ack > _snd.unacknowledged && data_ack <= _snd.next) {
            // Full ACK of segment
            while (!_snd.data.empty()
                    && (_snd.unacknowledged + _snd.data.front().data_len <= data_ack)) {
                _snd.unacknowledged += _snd.data.front().data_len;
                _snd.data.pop_front();
            }

            // Partial ACK of segment
            if (_snd.unacknowledged < data_ack) {
                // For simplicity sake, do not trim the partially acked data
                // off the unacked segment. So we do not need to recalculate
                // the tcp header when retransmit the segment, but the downside
                // is that we will retransmit the whole segment although part
                // of them are already acked.
                _snd.data.front().data_len -= (data_ack - _snd.unacknowledged);
                _snd.unacknowledged = data_ack;
            }

            // some data is acked, try send more data
            do_output_data = true;

            if (_snd.data.empty()) {
                // All outstanding segments are acked, turn off the timer.
                stop_retransmit_timer();
                // Signal the waiter of this event
                if (_snd._all_data_acked_promise) {
                    _snd._all_data_acked_promise->set_value();
                    _snd._all_data_acked_promise = {};
                }
            } else {
                // Restart the timer becasue new data is acked.
                start_retransmit_timer();
            }
        }
        if (_local_fin_sent && th->ack == _snd.next + 1) {
            _local_fin_acked = true;
            _snd.unacknowledged += 1;
            _snd.next += 1;
            if (both_closed()) {
                cleanup();
                return;
            }
        }
    }

    if (th->seq >= _snd.wl1 && th->ack >= _snd.wl2) {
        if (!_snd.window && th->window && _snd.unsent_len) {
            do_output_data = true;
        }
        _snd.window = th->window << _snd.window_scale;
        _snd.wl1 = th->seq;
        _snd.wl2 = th->ack;
    }
    // send some stuff
    if (do_output || (do_output_data && can_send())) {
        output();
    }
}

template <typename InetTraits>
packet tcp<InetTraits>::tcb::get_transmit_packet() {
    // easy case: empty queue
    if (_snd.unsent.empty()) {
        return packet();
    }
    auto can_send = this->can_send();
    // Max number of TCP payloads we can pass to NIC
    uint32_t len;
    if (_tcp.hw_features().tx_tso) {
        // FIXME: No magic numbers when adding IP and TCP option support
        // FIXME: Info tap device the size of the splitted packet
        len = _tcp.hw_features().max_packet_len - 20 - 20;
    } else {
        len = std::min(uint16_t(_tcp.hw_features().mtu - 20 - 20), _snd.mss);
    }
    can_send = std::min(can_send, len);
    // easy case: one small packet
    if (_snd.unsent.size() == 1 && _snd.unsent.front().len() <= can_send) {
        auto p = std::move(_snd.unsent.front());
        _snd.unsent.pop_front();
        _snd.unsent_len -= p.len();
        _snd.user_queue_space.signal(p.len());
        return p;
    }
    // moderate case: need to split one packet
    if (_snd.unsent.front().len() > can_send) {
        auto p = _snd.unsent.front().share(0, can_send);
        _snd.unsent.front().trim_front(can_send);
        _snd.unsent_len -= p.len();
        _snd.user_queue_space.signal(p.len());
        return p;
    }
    // hard case: merge some packets, possibly split last
    auto p = std::move(_snd.unsent.front());
    _snd.unsent.pop_front();
    can_send -= p.len();
    while (!_snd.unsent.empty()
            && _snd.unsent.front().len() <= can_send) {
        can_send -= _snd.unsent.front().len();
        p.append(std::move(_snd.unsent.front()));
        _snd.unsent.pop_front();
    }
    if (!_snd.unsent.empty() && can_send) {
        auto& q = _snd.unsent.front();
        p.append(q.share(0, can_send));
        q.trim_front(can_send);
    }
    _snd.unsent_len -= p.len();
    _snd.user_queue_space.signal(p.len());
    return p;
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::output() {
    uint8_t options_size = 0;
    packet p = get_transmit_packet();
    uint16_t len = p.len();

    if (!_local_syn_acked) {
        options_size = _option.get_size();
    }
    auto th = p.prepend_header<tcp_hdr>(options_size);
    th->src_port = _local_port;
    th->dst_port = _foreign_port;

    th->f_syn = !_local_syn_acked;
    _local_syn_sent |= th->f_syn;
    th->f_ack = _foreign_syn_received;
    if (th->f_ack) {
        clear_delayed_ack();
    }
    th->f_urg = false;
    th->f_psh = false;

    th->seq = _snd.next;
    th->ack = _rcv.next;
    th->data_offset = (sizeof(*th) + options_size) / 4;
    th->window = _rcv.window >> _rcv.window_scale;
    th->checksum = 0;

    _snd.next += len;

    // FIXME: does the FIN have to fit in the window?
    th->f_fin = _snd.closed && _snd.unsent_len == 0 && !_local_fin_acked;
    _local_fin_sent |= th->f_fin;

    // Add tcp options
    _option.fill(th, options_size);
    *th = hton(*th);

    checksummer csum;
    InetTraits::tcp_pseudo_header_checksum(csum, _local_ip, _foreign_ip, sizeof(*th) + options_size + len);
    if (_tcp.hw_features().tx_csum_offload) {
        // virtio-net's VIRTIO_NET_F_CSUM feature requires th->checksum to be
        // initialized to ones' complement sum of the pseudo header.
        th->checksum = ~csum.get();
    } else {
        csum.sum(p);
        th->checksum = csum.get();
    }

    offload_info oi;
    oi.protocol = ip_protocol_num::tcp;
    oi.tcp_hdr_len = sizeof(tcp_hdr) + options_size;
    p.set_offload_info(oi);

    if (len) {
        _snd.data.push_back({p.share(), len, 0});
        if (!_retransmit.armed()) {
            start_retransmit_timer();
        }
    }
    _tcp.send(_local_ip, _foreign_ip, std::move(p));
}

template <typename InetTraits>
future<> tcp<InetTraits>::tcb::wait_for_data() {
    if (!_rcv.data.empty() || _foreign_fin_received) {
        return make_ready_future<>();
    }
    _rcv._data_received_promise = promise<>();
    return _rcv._data_received_promise->get_future().then([this] {
        return make_ready_future<>();
    });
}

template <typename InetTraits>
future<> tcp<InetTraits>::tcb::wait_for_all_data_acked() {
    if (_snd.data.empty()) {
        return make_ready_future<>();
    }
    _snd._all_data_acked_promise = promise<>();
    return _snd._all_data_acked_promise->get_future().then([this] {
        return make_ready_future<>();
    });
}

template <typename InetTraits>
packet tcp<InetTraits>::tcb::read() {
    packet p;
    for (auto&& q : _rcv.data) {
        p.append(std::move(q));
    }
    _rcv.data.clear();
    return p;
}

template <typename InetTraits>
future<> tcp<InetTraits>::tcb::send(packet p) {
    // TODO: Handle p.len() > max user_queue_space case
    auto len = p.len();
    return _snd.user_queue_space.wait(len).then([this, p = std::move(p)] () mutable {
        _snd.unsent_len += p.len();
        _snd.unsent.push_back(std::move(p));
        if (can_send() > 0)
            output();
        return make_ready_future<>();
    });
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::close() {
    if (_snd.closed) {
        return;
    }
    // TODO: We should return a future to upper layer
    wait_for_all_data_acked().then([this]{
        _snd.closed = true;
        if (_snd.unsent_len == 0) {
            output();
        }
    });
}

template <typename InetTraits>
bool tcp<InetTraits>::tcb::should_send_ack() {
    if (_delayed_ack.cancel()) {
        return true;
    } else {
        _delayed_ack.arm(400ms);
        return false;
    }
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::clear_delayed_ack() {
    _delayed_ack.cancel();
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::merge_out_of_order() {
    if (_rcv.out_of_order.empty()) {
        return;
    }
    for (auto it = _rcv.out_of_order.begin(); it != _rcv.out_of_order.end();) {
        auto& p = it->second;
        auto seg_beg = it->first;
        auto seg_len = p.len();
        auto seg_end = seg_beg + seg_len;
        if (seg_beg <= _rcv.next && _rcv.next < seg_end) {
            // This segment has been received out of order and its previous
            // segment has been received now
            auto trim = _rcv.next - seg_beg;
            if (trim) {
                p.trim_front(trim);
                seg_len -= trim;
            }
            _rcv.next += seg_len;
            _rcv.data.push_back(std::move(p));
            // Since c++11, erase() always returns the value of the following element
            it = _rcv.out_of_order.erase(it);
        } else if (_rcv.next >= seg_end) {
            // This segment has been receive already, drop it
            it = _rcv.out_of_order.erase(it);
        } else {
            // seg_beg > _rcv.need, can not merge
            it++;
        }
    }
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::insert_out_of_order(tcp_seq seg, packet p) {
    bool insert;
    auto beg = seg;
    auto end = beg + p.len();
    for (auto it = _rcv.out_of_order.begin(); it != _rcv.out_of_order.end();) {
        auto& seg_pkt = it->second;
        auto seg_beg = it->first;
        auto seg_end = seg_beg + seg_pkt.len();
        // There are 6 cases:
        if (seg_beg <= beg && end <= seg_end) {
            // 1) seg_beg beg end seg_end
            // We already have data in this packet
            return;
        } else if (seg <= seg_beg && seg_end <= end) {
            // 2) beg seg_beg seg_end end
            // The new segment contains more data than this old segment
            // Delete the old one, insert the new one
            it = _rcv.out_of_order.erase(it);
            insert = true;
            break;
        } else if (beg < seg_beg && seg_beg <= end && end <= seg_end) {
            // 3) beg seg_beg end seg_end
            // Merge two segments, trim front of old segment
            auto trim = end - seg_beg;
            seg_pkt.trim_front(trim);
            p.append(std::move(seg_pkt));
            // Delete the old one, insert the new one
            it = _rcv.out_of_order.erase(it);
            insert = true;
            break;
        } else if (seg_beg <= beg && beg <= seg_end && seg_end < end) {
            // 4) seg_beg beg seg_end end
            // Merge two segments, trim front of new segment
            auto trim = seg_end - beg;
            p.trim_front(trim);
            // Append new data to the old segment, keep the old segment
            seg_pkt.append(std::move(p));
            insert = false;
            break;
        } else {
            // 5) beg end < seg_beg seg_end
            //   or
            // 6) seg_beg seg_end < beg end
            // Can not merge with this segment, keep looking
            it++;
            insert = true;
        }
    }

    if (insert) {
        p.linearize();
        _rcv.out_of_order.emplace(beg, std::move(p));
    }

    // TODO: With new segment merged with old segment, we might be able to
    // merge adjacent segments. Not urgent since merge_out_of_order will
    // consume these un-merged segments soon.
    return;
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::trim_receive_data_after_window() {
    abort();
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::retransmit() {
    if (_snd.data.empty()) {
        return;
    }

    // If there are unacked data, retransmit the earliest segment
    auto& unacked_seg = _snd.data.front();
    if (unacked_seg.nr_transmits < _max_nr_retransmit) {
        unacked_seg.nr_transmits++;
    } else {
        // Delete connection when max num of retransmission is reached
        cleanup();
        return;
    }
    // TODO: If the Path MTU changes, we need to split the segment if it is larger than current MSS
    _tcp.send(_local_ip, _foreign_ip, unacked_seg.p.share());

    // TODO: According to RFC 6298: Update RTO <- RTO * 2, when we support dynamic RTO calculation
    start_retransmit_timer();
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::cleanup() {
    _snd.data.clear();
    _rcv.out_of_order.clear();
    stop_retransmit_timer();
    clear_delayed_ack();
    remove_from_tcbs();
}

template <typename InetTraits>
void tcp<InetTraits>::connection::close_read() {
}

template <typename InetTraits>
void tcp<InetTraits>::connection::close_write() {
    _tcb->close();
}

template <typename InetTraits>
constexpr uint16_t tcp<InetTraits>::tcb::_max_nr_retransmit;


}



#endif /* TCP_HH_ */
