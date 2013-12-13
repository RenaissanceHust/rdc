CLR remote differential compression algorithm
===

Branch from the source code of the Lana distributed synchronization application [https://code.google.com/p/lanaisel/](https://code.google.com/p/lanaisel/).

This algorithm is a variant of the third (and final) deltas transfer algorithm described in the PhD thesis of the RSYNC application author. This algorihtm uses the MD5 cryptografic hash function and a rolling checksum hash function. Each file transfer takes a maximum of two passes on the original file. This file transfer algorithm is *not* compatible with the RSYNC application described in the paper. Maximum file size of 4GiB. This is only an algorithm implementation in code library form, not the whole RSYNC application. It also features a number of improvements over the original algorithm.
