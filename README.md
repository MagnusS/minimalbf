Minimal Bloom filter implementation
=========

This is a minimal Bloom filter implementation in Python. 

See example.py for usage examples.

Minimal Bloom filters used for set reconciliation are described in our paper:

Magnus Skjegstad and Torleiv Maseng. 2011. Low complexity set reconciliation using Bloom filters. In Proceedings of the 7th ACM ACM SIGACT/SIGMOBILE International Workshop on Foundations of Mobile Computing (FOMC '11). ACM, New York, NY, USA, 33-41. DOI=10.1145/1998476.1998483 

http://doi.acm.org/10.1145/1998476.1998483 

Short summary:

The minimal Bloom filter has a 50% false positive probability, but by changing the number of padding bits, the actual items that collide (produce false positives) can be varied. This allows us to create a pipeline of minimal Bloom filters that each have a 50% false positive probability, but when combined results in a much lower false positive probability. The positive probability of the pipeline can be dynamically adapted without requiring additional hash functions. This makes it useful for set reconciliation in computer networks where minimal Bloom filters can be exchanged until the sets are equal.

