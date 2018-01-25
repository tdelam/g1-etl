#
# Building + Running
#
# ~$ docker build -t g1-etl .
# ~$ docker run -p 5000:5000 --name g1-etl g1-etl
#
# Additional parameters:
# - `-v $PWD:/workspace`: If you want to use this image for development, run the
#     image with this parameter to mount your local copy to the workspace
#     contained within.
# - `ash`: If you want to gain access to the environment in order to run your
#     own commands (recommended during development), append `ash` to the run
#     command in order to start a shell instead of the flask server.
#

FROM python:2.7.14-alpine3.7

EXPOSE 5000
ENV FLASK_APP=/workspace/mmj/server.py

# Create unprivileged user to run server
RUN addgroup -g 1000 py \
 && adduser -u 1000 -G py -s /bin/sh -D py \
 && mkdir /workspace \
 && chown py. /workspace

WORKDIR /workspace
COPY . /workspace

RUN apk update \

	# Add build-time dependencies, and create a virtual name for them so we can
 	# remove them later
 	&& apk --no-cache add --virtual build-dependencies \
		build-base \
		py-mysqldb \
		gcc \
		libc-dev \
		libffi-dev \
		mariadb-dev \
		python-dev \

 	# Install application requirements
 	&& pip install -r requirements.txt \

 	# Remove pip cache. Drops image size by >50%.
 	&& rm -rf ~/.cache/pip \

 	# Add runtime dependencies
 	&& apk --no-cache add mariadb-client-libs \

 	# Remove build dependencies
 	&& apk del build-dependencies

USER py
CMD [ "flask", "run" ]