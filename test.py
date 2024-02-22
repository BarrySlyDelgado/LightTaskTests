#!/usr/bin/env python

# Copyright (C) 2022- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This example shows some of the data handling features of taskvine.
# It performs a BLAST search of the "Landmark" model organism database.
# It works by constructing tasks that download the blast executable
# and landmark database from NCBI, and then performs a short query.

# Each task in the workflow performs a query of the database using
# 16 (random) query strings generated at the manager.
# Both the downloads are automatically unpacked, cached, and shared
# with all the same tasks on the worker.

import ndcctools.taskvine as vine
import time
import os
def num(num):
    import numpy
    print('Hello World!')
    return(numpy.__file__ +' '+ str(num))
    
m = vine.Manager(port=9125, run_info_path=f"/project01/ndcms/{os.environ['USER']}/vine-run-info")
m.set_name('bslydelg-test')
m.tune('wait-for-workers', 20)
print("Declaring tasks...")
tasks = 5000
for i in range(tasks):
    t = vine.PythonTask(num, 5)
    t.set_cores(1)
    #t.light_task()
    m.submit(t)
print('waiting')
while not m.empty():
    t = m.wait(5)
    if t:
        if t.successful():
            pass
print("all tasks complete!")
