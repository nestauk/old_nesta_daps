Containerised Luigi
===================

Requirements
------------

To containerise a pipeline a few steps are required:

- All imports must be absolute, ie :code:`from nesta.` packages, core etc
- Once testing is complete the code should be committed and pushed to github, as this prevents the need to use local build options
- If building and running locally, Docker must be installed on the local machine and given enough RAM in the settings to run the pipeline
- Any required configuration files must be in :code:`nesta.core.config` ie luigi and mysql config files, any API keys.
  **This directory is ignored but check before committing**

Build
-----

The build uses a multi-stage Dockerfile to reduce the size of the final image:

1. Code is git cloned and requirements are pip installed into a virtual environment
2. The environment is copied into the second image

From the root of the repository:
:code:`docker build -f docker/Dockerfile -t name:tag .`

Where :code:`name` is the name of the created image and :code:`tag` is the chosen tag.
Eg :code:`arxlive:latest`. This just makes the run step easier rather than using the generated id

The two stage build will normally just rebuild the second stage pulling in new code only.
If a full rebuild is required, eg after requirements.txt has changed then include:
:code:`--no-cache`

Python version defaults to 3.6 but can be set during build by including the flag:
:code:`--build-arg python-version=3.7`

Tag defaults to `dev` but this can be overridden by including the flag:
:code:`--build-arg GIT_TAG=0.3` a branch name also works :code:`--build-arg GIT_TAG=my-branch`

Work from a branch or locally while testing. Override the target branch from Github using the method above, or use the commented out code in the Dockerfile to switch to build from local files. **Don't commit this change!**

Run
---

As only one pipeline runs in the container the :code:`luigid` scheduler is not used.

There is a :code:`docker-compose` file for arXlive which mounts your local ~.aws folder for AWS credentials as this outside docker's context:

:code:`docker-compose -f docker/docker-compose.yml run luigi --module module_path params`

Where:

- :code:`docker-compose.yml` is the docker-compose file containing the image: :code:`image_name:tag` from the build
- :code:`module_path` is the full python path to the module
- :code:`params` are any other params to supply as per normal, ie :code:`--date` :code:`--production` etc

Eg: :code:`docker-compose -f docker/docker-compose-arxlive-dev.yml run luigi --module nesta.core.routines.arxiv.arxiv_iterative_root_task RootTask --date 2019-04-16`

This could be adapted for each pipeline, or alternatively run with the volume specified
with :code:`-v`

:code:`docker run -v ~/.aws:/root/.aws:ro name:tag --module ...`

Where :code:`name` is the name of the created image and :code:`tag` is the chosen tag.
Eg :code:`arxlive:latest`
:code:`--module ...` onwards contains the arguments you would pass to Luigi.


Scheduling
----------

1. Create an executable shell script in :code:`nesta.core.scripts` to launch docker-compose with all the necessary parameters eg: production
2. Add a cron job to the shell script (there are some good online cron syntax checking tools, if needed)
3. Set the cron job to run every few minutes while testing and check the logs with :code:`docker logs mycontainterhash --tail 50`. Obtain the hash using :code:`docker ps`
4. It will run logged in as the user who set it up but there still may still be some permissions issues to deal with


Currently scheduled
"""""""""""""""""""

arXlive:

- A shell script to launch docker-compose for arXlive is set up to run in a cron job on user :code:`russellwinch`
- This is scheduled for Sunday-Thursday at 0300 GMT. arXiv is updated on these days at 0200 GMT
- Logs are just stored in the container, use :code:`docker logs container_name` to view

Important points
----------------

- Keep any built images secure, they contain credentials
- You need to rebuild if code has changed
- As there is no central scheduler there is nothing stopping you from running the task more than once at the same time, by launching the container multiple times
- The graphical interface is not enabled without the scheduler

Debugging
---------

If necessary, it's possible to debug inside the container, but the :code:`endpoint` needs to be overridden with :code:`bash`:

:code:`docker run --entrypoint /bin/bash -itv ~/.aws:/root/.aws:ro image_name:tag`

Where :code:`image_name:tag` is the image from the build step
This includes the mounting  of the .aws folder

Almost nothing is installed (not even vi!!) other than Python so :code:`apt-get update` and then :code:`apt-get install` whatever you need

Todo
----

A few things are sub-optimal:

- The container runs on the prod box rather than in the cloud in ECS
- Credentials are held in the container and local AWS config is required, this is the cause of the above point
- Due to the Nesta monorepo everything is pip installed, making a large container size with many unrequired packages. Pipeline specific requirements should be considered
- As logs are stored in the old containers they are kept until the next run where they are pruned and the logs are lost.  Add a method of getting the logs to the host logger and record centrally
- In the arXlive pipeline there are at least 500 calls to the MAG API each run as the process tries to pick up new title matches on
  existing articles. As the API key only allows 10,000 calls per month this is currently OK with the schedule as it is but could possibly go over at some point
