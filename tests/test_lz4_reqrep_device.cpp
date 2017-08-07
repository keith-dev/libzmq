/*
    Copyright (c) 2007-2017 Contributors as noted in the AUTHORS file

    This file is part of libzmq, the ZeroMQ core engine in C++.

    libzmq is free software; you can redistribute it and/or modify it under
    the terms of the GNU Lesser General Public License (LGPL) as published
    by the Free Software Foundation; either version 3 of the License, or
    (at your option) any later version.

    As a special exception, the Contributors give you permission to link
    this library with independent modules to produce an executable,
    regardless of the license terms of these independent modules, and to
    copy and distribute the resulting executable under terms of your choice,
    provided that you also meet, for each linked independent module, the
    terms and conditions of the license of that module. An independent
    module is a module which is not derived from or based on this library.
    If you modify this library, you must extend this exception to your
    version of the library.

    libzmq is distributed in the hope that it will be useful, but WITHOUT
    ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
    FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public
    License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "testutil.hpp"
#include <vector>
#include <memory>
#include <algorithm>
#include <stdlib.h>

#define LOGFILE "test_lz4_reqrep_device.log"

typedef std::vector<char> buffer_t;
typedef std::vector<std::unique_ptr<buffer_t>> buffers_t;

const int hwm = 30 * 1024;
const size_t maxsz = 10 * 1024;

int rc;
size_t len = MAX_SOCKET_STRING;
char endpoint1[MAX_SOCKET_STRING];
char endpoint2[MAX_SOCKET_STRING];

// build alphabetic string
std::unique_ptr<buffer_t> build_alpha (int size)
{
    buffer_t* vec = new buffer_t(size);

    for (int i = 0; i != size; ++i)
    {
        if ((i % 2*26) < 26)
            (*vec)[i] = 'A' + (i % 26);
        else
            (*vec)[i] = 'a' + (i % 26);
    }
    
    return std::unique_ptr<buffer_t>(vec);
}

// build numeric string
std::unique_ptr<buffer_t> build_num (int size)
{
    buffer_t* vec = new buffer_t(size);

    for (int i = 0; i != size; ++i)
        (*vec)[i] = '0' + (i % 10);
    
    return std::unique_ptr<buffer_t>(vec);
}

// build random binary string
std::unique_ptr<buffer_t> build_bin (int size)
{
    buffer_t* vec = new buffer_t(size);

    for (int i = 0; i != size; ++i)
        (*vec)[i] = rand() % 256;
    
    return std::unique_ptr<buffer_t>(vec);
}

// switch hwm send
int send_hwm (const char* name, void* sock, int val)
{
    rc = zmq_setsockopt (sock, ZMQ_SNDHWM, &val, sizeof (val));
    if (FILE* f = fopen (LOGFILE, "a+"))
    {
        fprintf (f, "%s: setsocketopt(ZMQ_SNDHWM, %d)\n", name, val);
        fclose (f);
    }

    return rc;
}

// switch hwm recv
int recv_hwm (const char* name, void* sock, int val)
{
    rc = zmq_setsockopt (sock, ZMQ_RCVHWM, &val, sizeof (val));
    if (FILE* f = fopen (LOGFILE, "a+"))
    {
        fprintf (f, "%s: setsocketopt(ZMQ_RCVHWM, %d)\n", name, val);
        fclose (f);
    }
    
    return rc;
}

// switch lz4 send
int send_lz4 (const char* name, void* sock, int val)
{
    rc = zmq_setsockopt (sock, ZMQ_SNDLZ4, &val, sizeof (val));
    if (FILE* f = fopen (LOGFILE, "a+"))
    {
        fprintf (f, "%s: setsocketopt(ZMQ_SNDLZ4, %d)\n", name, val);
        fclose (f);
    }

    return rc;
}

// switch lz4 recv
int recv_lz4 (const char* name, void* sock, int val)
{
    rc = zmq_setsockopt (sock, ZMQ_RCVLZ4, &val, sizeof (val));
    if (FILE* f = fopen (LOGFILE, "a+"))
    {
        fprintf (f, "%s: setsocketopt(ZMQ_RCVLZ4, %d)\n", name, val);
        fclose (f);
    }
    
    return rc;
}

void* create_dealer (void* ctx)
{
    void *dealer = zmq_socket (ctx, ZMQ_DEALER);
    assert (dealer);
    rc = zmq_bind (dealer, "tcp://127.0.0.1:*");
    assert (rc == 0);

    len = sizeof (endpoint1);;
    rc = zmq_getsockopt (dealer, ZMQ_LAST_ENDPOINT, endpoint1, &len);
    assert (rc == 0);

    rc = recv_hwm("dealer", dealer, hwm);
    assert (rc == 0);

    rc = send_hwm("dealer", dealer, hwm);
    assert (rc == 0);

    return dealer;
}

void* create_router (void* ctx)
{
    void *router = zmq_socket (ctx, ZMQ_ROUTER);
    assert (router);
    rc = zmq_bind (router, "tcp://127.0.0.1:*");
    assert (rc == 0);

    len = sizeof (endpoint2);
    rc = zmq_getsockopt (router, ZMQ_LAST_ENDPOINT, endpoint2, &len);
    assert (rc == 0);

    rc = recv_hwm("router", router, hwm);
    assert (rc == 0);

    rc = send_hwm("router", router, hwm);
    assert (rc == 0);

    return router;
}

void* create_rep (void* ctx)
{
    //  Create a worker.
    void *rep = zmq_socket (ctx, ZMQ_REP);
    assert (rep);

    rc = zmq_connect (rep, endpoint1);
    assert (rc == 0);

    rc = recv_lz4 ("rep", rep, 1);
    assert (rc == 0);

    rc = send_lz4 ("rep", rep, 1);
    assert (rc == 0);

    rc = recv_hwm("rep", rep, hwm);
    assert (rc == 0);

    rc = send_hwm("rep", rep, hwm);
    assert (rc == 0);

    return rep;
}

void* create_req (void* ctx)
{
    //  Create a client.
    void *req = zmq_socket (ctx, ZMQ_REQ);
    assert (req);

    rc = zmq_connect (req, endpoint2);
    assert (rc == 0);

    rc = recv_lz4 ("req", req, 1);
    assert (rc == 0);

    rc = send_lz4 ("req", req, 1);
    assert (rc == 0);

    rc = recv_hwm("req", req, hwm);
    assert (rc == 0);

    rc = send_hwm("req", req, hwm);
    assert (rc == 0);

    return req;
}

void send (const char* name, void* req, const buffers_t& inbufs)
{
    //  Send a request.
    bool more = false;
    size_t count = 0;
    for (auto& pbuf : inbufs)
    {
        more = (inbufs.size () - 1) != count;

        rc = zmq_send (req, pbuf->data (), pbuf->size (), more ? ZMQ_SNDMORE : 0);
        if (FILE* f = fopen (LOGFILE, "a+"))
        {
            fprintf (f, "%s: send rc=%d in_size=%zu more=%d\n", name, rc, pbuf->size (), (int)more);
            fclose (f);
        }
        assert (rc != -1);
        assert ((size_t)rc == pbuf->size ());

        ++count;
    }
    assert (more == false);
}

void recv_and_check (const char* name, void* rep, const buffers_t& inbufs)
{
    buffers_t outbufs;

    //  Receive the request.
    int rcvmore = 1;
    for (size_t i = 0; rcvmore; ++i)
    {
        outbufs.push_back( std::unique_ptr<buffer_t> (new buffer_t (maxsz)) );
        buffer_t& out = *outbufs[ outbufs.size () - 1 ];

        rc = zmq_recv (rep, out.data (), out.size (), 0);
        if (FILE* f = fopen (LOGFILE, "a+"))
        {
            fprintf (f, "%s: recv rc=%d\n", name, rc);
            fclose (f);
        }
        assert (rc != -1);
        out.resize (rc);

        assert (out.size () == inbufs[ i ]->size ());
        assert (memcmp (inbufs[ i ]->data (), out.data (), out.size ()) == 0);

        size_t sz = sizeof (rcvmore);
        rc = zmq_getsockopt (rep, ZMQ_RCVMORE, &rcvmore, &sz);
        assert (rc == 0);
    }
}

void close_object (void* obj)
{
    //  Clean up.
    rc = zmq_close (obj);
    assert (rc == 0);
}

int main (void)
{
    srand(0);
    setup_test_environment ();
    if (FILE* f = fopen (LOGFILE, "a+"))
    {
        fprintf (f, "new test\n");
        fclose (f);
    }
    
    // create context
    void *ctx = zmq_ctx_new ();
    assert (ctx);

    //  Create a req/rep device.
    void* dealer = create_dealer (ctx);
    void* router = create_router (ctx);
    void* rep = create_rep (ctx);
    void* req = create_req (ctx);

    // test payload
    buffers_t reference_data;
    reference_data.push_back ( build_alpha (4 * 1024) );
    reference_data.push_back ( build_num (2 * 1024) );
    reference_data.push_back ( build_alpha (8 * 1024) );
    reference_data.push_back ( build_num (6 * 1024) );
    reference_data.push_back ( build_bin (8 * 1024) );
    {
        size_t sum = 0;
        for (auto p = reference_data.begin (); p != reference_data.end (); ++p)
            sum += p->get()->size ();
        assert (sum < (size_t)hwm);
    }

    //  Send a request.
    send ("req", req, reference_data);

    //  Pass the request through the device.
    for (size_t i = 0; i != 2 + reference_data.size (); ++i) {
        zmq_msg_t msg;
        rc = zmq_msg_init (&msg);
        assert (rc == 0);
        rc = zmq_msg_recv (&msg, router, 0);
        if (FILE* f = fopen (LOGFILE, "a+"))
        {
            fprintf (f, "router: recv rc=%d\n", rc);
            fclose (f);
        }
        assert (rc >= 0);

        bool end_of_header = zmq_msg_size (&msg) == 0;

        int rcvmore;
        size_t sz = sizeof (rcvmore);
        rc = zmq_getsockopt (router, ZMQ_RCVMORE, &rcvmore, &sz);
        assert (rc == 0);

        rc = zmq_msg_send (&msg, dealer, rcvmore? ZMQ_SNDMORE: 0);
        assert (rc >= 0);
        if (FILE* f = fopen (LOGFILE, "a+"))
        {
            fprintf (f, "dealer: send rc=%d more=%d\n", rc, rcvmore);
            fclose (f);
        }

		if (end_of_header)
        {
        	// next messages are payload, compression the payload
            rc = send_lz4 ("dealer", dealer, 1);
            assert (rc == 0);
        }
    }

    // device compression off
    rc = recv_lz4 ("dealer", dealer, 0);
    assert (rc == 0);

    rc = send_lz4 ("dealer", dealer, 0);
    assert (rc == 0);

    //  Receive the request.
    recv_and_check ("rep", rep, reference_data);

    //  Send the reply.
    send ("rep", rep, reference_data);

    //  Pass the reply through the device.
    for (size_t i = 0; i != 2 + reference_data.size (); ++i) {
        zmq_msg_t msg;
        rc = zmq_msg_init (&msg);
        assert (rc == 0);
        rc = zmq_msg_recv (&msg, dealer, 0);
        if (FILE* f = fopen (LOGFILE, "a+"))
        {
            fprintf (f, "router: recv rc=%d\n", rc);
            fclose (f);
        }
        assert (rc >= 0);

        bool end_of_header = zmq_msg_size (&msg) == 0;

        int rcvmore;
        size_t sz = sizeof (rcvmore);
        rc = zmq_getsockopt (dealer, ZMQ_RCVMORE, &rcvmore, &sz);
        assert (rc == 0);

        rc = zmq_msg_send (&msg, router, rcvmore? ZMQ_SNDMORE: 0);
        if (FILE* f = fopen (LOGFILE, "a+"))
        {
            fprintf (f, "dealer: send rc=%d more=%d\n", rc, rcvmore);
            fclose (f);
        }
        assert (rc >= 0);

		if (end_of_header)
        {
            // next messages are payload, decompression the payload
            rc = recv_lz4 ("dealer", dealer, 1);
            assert (rc == 0);
        }
    }

    // device compression off
    rc = recv_lz4 ("dealer", dealer, 0);
    assert (rc == 0);

    rc = send_lz4 ("dealer", dealer, 0);
    assert (rc == 0);

    //  Receive the reply.
    recv_and_check ("req", req, reference_data);

    //  Clean up.
    close_object (rep);
    close_object (req);
    close_object (router);
    close_object (dealer);

    return 0;
}
