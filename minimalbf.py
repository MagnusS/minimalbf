#!/usr/bin/env python

'''
Minimal Bloom filter implementation in Python.

See http://github.com/MagnusS/minimalbf for the latest version.

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
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
'''

''' Pipelined Minimal Bloom filters. Creates K minimal Bloom filters, each with one more padding bit than the previous. The minimal Bloom filters will each have a false positive probability of about 50%, but as the false positives vary between the filters (due to padding bits) they can be combined to reduce the total false positive probability '''

class MinimalBFPipe(object):
        bfs = None
        check = None

        ''' Create a minimal BF pipe with K minimal Bloom filters, starting with init_padding_bits padding bits. Use verify=True to 
        verify that we never have false negatives (only for debugging) '''
        def __init__(self, list_of_hashes, init_padding_bits=0, k=1, verify=False):
                self.verify = verify
                if self.verify:
                        self.check = set()
                        for h in list_of_hashes:
                                self.check.add(h)

                self.bfs = []
                for i in range(0,k):
                        self.bfs.append(MinimalBF(list_of_hashes, padding_bits=init_padding_bits + i))

        ''' Check whether and item hash is in ALL Bloom filters in the pipeline. If not, return false. '''
        def hasitem(self, itemhash):
                for bf in self.bfs:
                        if not bf.hasitem(itemhash):
                                if self.check != None and itemhash in self.check:
                                        raise Exception("Bloom filter had a false negative. This should never happen.")
                                return False
                return True

        ''' Calculate the total size in bytes of the Bloom filters in the pipeline.'''
        def calc_size(self):
                s = 0
                for bf in self.bfs:
                        s = s + bf.calc_size()
                return s

''' Minimal Bloom filter with approx. 50% false positive probability. Increase padding bits to vary which bits collide. '''
class MinimalBF(object):
        m = None
        n = None
        bits = None

        ''' Initialize the minimal Bloom filter with a list of hashes and a number of padding_bits. The hashes should be randomly distributed and much larger than the length of the minimal Bloom filter. '''
        def __init__(self, list_of_hashes, padding_bits=0):
                self.n = len(list_of_hashes)

                if self.n <= 0:
                        raise Exception("MinimalBF items must be > 0")

                self.m = int(round(1.44 * self.n) + padding_bits)
                self.bits = [False] * self.m

                for h in list_of_hashes:
                        pos = abs(long(h)) % self.m
                        self.bits[pos] = True

        def __repr__(self):
                return str(self.bits)

        ''' Returns true if the item is in the Bloom filter with 50% probabiliy, returns false if the item is not in the Bloom filter '''
        def hasitem(self,itemhash):
                pos = abs(long(itemhash)) % self.m
                return self.bits[pos]

        ''' Return the number of bytes used by this Bloom filter '''
        def calc_size(self):
                return int(math.ceil(self.m / 8.0)) # may return fractions due to padding. Assume we have to store the last byte always.

### TESTS ###

import unittest
import random
import math

class TestMinimalBF(unittest.TestCase):
        def setUp(self):
                self.hashes = []
                for f in range(10000):
                        self.hashes.append(random.randrange(0,2**32))
                self.bf = MinimalBF(self.hashes, padding_bits=0)

        def test_calc_size(self):
                bf_size = self.bf.m / 8
                self.assertEqual(bf_size, self.bf.calc_size())

        def test_hasitem(self):
                for f in self.hashes:
                        self.assertTrue(self.bf.hasitem(f))

        def test_fp(self):
                hit = 0
                miss = 0
                tests = 500000
                for i in range(tests):
                        if self.bf.hasitem(random.randrange(0,2**32)):
                                hit = hit + 1
                        else:
                                miss = miss + 1
                fp_p = float(hit) / tests # should be approx 0.5 (0.49 - 0.51 is fine)
                self.assertAlmostEqual(0.50, fp_p, 1)

class TestMinimalBFPipe(unittest.TestCase):
        k = 10
        def setUp(self):
                self.hashes = []
                for f in range(10000):
                        self.hashes.append(random.randrange(0,2**32))
                self.bf = MinimalBFPipe(self.hashes, init_padding_bits=0, k=self.k)

        def test_calc_size(self):
                bf_size = 0
                for f in self.bf.bfs:
                        bf_size = bf_size + int(math.ceil(f.m / 8.0))
                self.assertEqual(bf_size, self.bf.calc_size())

        def test_hasitem(self):
                for f in self.hashes:
                        self.assertTrue(self.bf.hasitem(f))

        def test_fp(self):
                # estimate fp based on using k hash functions
                n = len(self.hashes)
                k = self.k
                m = 1.44 * len(self.hashes) * self.k + sum(i for i in range(k)) # add +1 extra bit for k bf's
                estimated_fp = (1.0 - (1.0 - (1/m))**(k*n))**k

                # test one hash function result
                hit = 0
                miss = 0
                tests = 1000000
                for i in range(tests):
                        if self.bf.hasitem(random.randrange(0,2**32)):
                                hit = hit + 1
                        else:
                                miss = miss + 1
                fp_p = float(hit) / tests 

                print "measured fp:",fp_p, "estimated fp:",estimated_fp," ",
        
                self.assertAlmostEqual(estimated_fp, fp_p, 3)

if __name__ == '__main__':
        suite = unittest.TestLoader().loadTestsFromTestCase(TestMinimalBF)
        unittest.TextTestRunner(verbosity=2).run(suite)
        suite = unittest.TestLoader().loadTestsFromTestCase(TestMinimalBFPipe)
        unittest.TextTestRunner(verbosity=2).run(suite)
