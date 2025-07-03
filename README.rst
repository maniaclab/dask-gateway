dask-gateway
============

A multi-tenant server for securely deploying and managing Dask clusters. See
`the documentation <https://gateway.dask.org/>`__ for more information.


=======================================================
HTCondor-enabled Dask Kubernetes Controller
=======================================================

This repository extends the upstream Dask Kubernetes controller to add
support for dynamically provisioning Dask workers on an HTCondor cluster.
The enhancements allow Dask clusters to be elastically scaled using HTCondor
jobs instead of directly launching Kubernetes pods for workers.

Key Features
============

- **HTCondor Integration**
  - Submit Dask worker jobs as HTCondor jobs using `condor_submit`.
  - Query job status with `condor_q`, and cancel with `condor_rm`.
  - Supports HTCondor ClassAds for flexible job attributes.
  - Tracks job IDs and job names using a consistent naming counter.

- **User Context Switching**
  - Uses a `run_as_user` mechanism to submit HTCondor jobs as the correct user.
  - Integrates with CI Connect to look up the proper Unix username based on email.

- **Improved Worker Management**
  - Keeps a per-cluster job counter to avoid name collisions when resubmitting workers.
  - Allows Dask workers to run in containers via HTCondor’s Docker universe.

- **Scheduler Options**
  - Passes Dask scheduler and dashboard ports seamlessly.
  - Configures environment variables for correct cluster linking.

- **Job Summary and Status**
  - Includes a robust JSON parser for `condor_q -json` output
  - Provides summarized counts of pending, running, held, and completed jobs.

- **Enhanced Logging**
  - Consistent, namespace-aware logs to track submitted HTCondor jobs
  - Logs current counters, job batches, and user lookup details

Design Motivation
==================

Kubernetes clusters sometimes face resource or scheduling limitations
for large Dask workloads. By leveraging an HTCondor pool, this controller
can offload Dask workers to traditional HPC-style batch nodes while
maintaining the benefits of a Kubernetes-based Dask scheduler.

The enhancements in this repo allow Dask Gateway to flexibly manage
compute resources across both Kubernetes and HTCondor infrastructures.

Getting Started
================

1. Install dependencies
   (see `requirements.txt` if present)
   
2. Configure HTCondor with:
   
   - Docker universe enabled
   - a suitable submit node
   - required transfer input files (e.g., Dask environment scripts)

3. Configure the Kubernetes cluster with:
   
   - Dask Gateway
   - an external scheduler service reachable by HTCondor workers

4. Deploy the controller:
   
   .. code-block:: bash
   
      python controller.py

5. Launch your Dask cluster via the usual gateway interface.

Usage Notes
===========

- Ensure CI Connect is properly configured to resolve user identities.
- The controller expects HTCondor submit nodes to have access to
  `condor_submit`, `condor_q`, and `condor_rm` commands.
- Job counters are managed on a per-cluster basis and restart from
  0 if no jobs are detected in the query.
- Logs will appear in the Kubernetes controller logs (e.g., via
  `kubectl logs`) for debugging and tracking job progress.

Contributing
============

Pull requests are welcome! Please open issues if you discover bugs or have suggestions for additional features.


LICENSE
=======

New BSD. See the `License File
<https://github.com/dask/dask-gateway/blob/main/dask-gateway-server/LICENSE>`_.

