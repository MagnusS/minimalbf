#!/usr/bin/env python

'''
Copyright (c) 2014, Magnus Skjegstad
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the {organization} nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE
'''

from minimalbf import MinimalBF, MinimalBFPipe
import random

# Set reconciliation example with two nodes
#
# Creates a random set in node A with 10000 entries. Then copies half of the set 
# to node B and attempts to use minimal Bloom filters to discover the rest of the 
# missing items. 

set_size = 100000

# Create a list of hashes for node A (random numbers from 0 to 2^32)
node_A_hashes = set()
while len(node_A_hashes) < set_size:
        node_A_hashes.add(random.randrange(0,2**32))


# Node B has a random subset of Node A's set
node_B_hashes = set(random.sample(node_A_hashes, set_size/2))

print "#### Starting reconciliation without pipelining... ####"
print
print "Node A has",len(node_A_hashes),"items"
print "Node B has",len(node_B_hashes),"items"
print

# Perform set reconciliation

padding_bits = 0
iteration = 0
while node_A_hashes != node_B_hashes:
        iteration = iteration + 1
        print "# Iteration", iteration

        # Node B sends its minimal Bloom filter to A
        bf_from_B = MinimalBF(node_B_hashes, padding_bits)
        print "NODE B: Sent",bf_from_B.calc_size(),"bytes to A"

        # Node A checks its items against the Bloom filter and sends missing items to B
        send_to_B = set()
        for h in node_A_hashes:
                if not bf_from_B.hasitem(h):
                        send_to_B.add(h)
        print "NODE A: Sent",len(send_to_B),"items to B"

        # Node B merge items from A
        for h in send_to_B:
                node_B_hashes.add(h)

        print "NODE B: Now missing",len(node_A_hashes) - len(node_B_hashes),"items"

        # Increase padding bits
        padding_bits = padding_bits + 1

print
print "Done! Took",iteration,"iteration(s) to synchronize A and B."


# Redo experiment with pipeline (sends all BFs at once)
pipe_length = 25

# Reset Node B to a random subset of Node A's set
node_B_hashes = set(random.sample(node_A_hashes, set_size/2))

print
print "#### Starting reconciliation with pipelining... ####"
print 
print "Send",pipe_length,"BFs in each iteration."
print "Node A has",len(node_A_hashes),"items"
print "Node B has",len(node_B_hashes),"items"
print

# Perform set reconciliation

padding_bits = 0
iteration = 0
while node_A_hashes != node_B_hashes:
        iteration = iteration + 1
        print "# Iteration", iteration

        # Node B sends its minimal Bloom filter to A
        bf_from_B = MinimalBFPipe(node_B_hashes, padding_bits, pipe_length)
        print "NODE B: Sent",bf_from_B.calc_size(),"bytes to A"

        # Node A checks its items against the Bloom filter and sends missing items to B
        send_to_B = set()
        for h in node_A_hashes:
                if not bf_from_B.hasitem(h):
                        send_to_B.add(h)
        print "NODE A: Sent",len(send_to_B),"items to B"

        # Node B merge items from A
        for h in send_to_B:
                node_B_hashes.add(h)

        print "NODE B: Now missing",len(node_A_hashes) - len(node_B_hashes),"items"

        # Increase padding bits
        padding_bits = padding_bits + pipe_length # add one bit per BF in pipeline

print
print "Done! Took",iteration,"iteration(s) to synchronize A and B."

