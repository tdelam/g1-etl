# g1-etl
GrowOne MMJ ETL

## Building And Running

### With Docker

*Note:* These instructions are also available in the Dockerfile itself. If a discrepancy is found, the Dockerfile is generally considered to be more up-to-date, because everybody forgets about READMEs, right? Now, on to business...

Building and developing with Docker is very simple. The first step is to build the image upon which the container is built:

```
~$ docker build -t g1-etl .
```

The build command will pull in MySQL, Python, and all the libraries that are needed in order to run the application. Once the build is done, it will remove MySQL server and clean the PIP cache in order to save space.

Now you get to run the container. There are two primary ways of running the container...

#### For Production

```
~$ docker run -p 5000:5000 --name g1-etl g1-etl
```

This will build the container, and automatically run the Flask server contained within. Nothing else required.

#### For Development

```
~$ docker run -it -p 5000:5000 -v $PWD:/workspace --name g1-etl g1-etl ash
```

There are some differences between this command and the command for production:

* `-it` - Attaches STDIO to the container. Without this, you won't be able to get a terminal within the container (that's the `ash` part at the end).
* `-v $PWD:/workspace` - Assuming you run the container while located in your local copy directory, this will mount your local copy as the workspace within the Docker container. Changes in your local copy will be reflected in the container, and vice-versa.
* `ash` - Starts a shell session in the container instead of the Flask server. This requires the `-it` options to be present, otherwise the shell session will exit immediately and confusion will reign.

### With Docker Compose

If you've installed Docker Engine, then you've got Docker Compose! Docker Compose is used for production deployments, in conjunction with Rancher. But you can use it for development too! In fact, it's really cool for development because it gives us the option of spinning up not only ETL, but POS _and_ MMJM at the same time, giving us a complete, end-to-end testing environment (in the future anyway). Want to use Docker Compose? Try the following on for size:

```
~$ docker compose up
```

This will build the ETL environment, add all the environment variables specified in the `.env` file, and spin up the Flask server for you. If you still want shell access, you can run

```
~$ docker exec -it g1etl_etl_1 ash
```

to gain a shell.

#### Set up Python Virtualenv
`$ [sudo] pip install virtualenv`

#### Set up virtualenvwrapper (optional but recommended)
`$ [sudo] pip install virtualenvwrapper`
`$ export WORKON_HOME=~/Envs`
`$ mkdir -p $WORKON_HOME`
`$ source /usr/local/bin/virtualenvwrapper.sh`
`$ mkvirtualenv g1-etl`

Whenever you want to work on the `g1-etl` project you simply type `workon g1-etl`. Add the following hook to `~/Envs/postactivate`

```
#!/bin/zsh
# This hook is sourced after every virtualenv is activated.

proj_name=$(echo $VIRTUAL_ENV|awk -F'/' '{print $NF}')
cd ~/Envs/$proj_name

cd () {
    if (( $# == 0 ))
    then
        builtin cd $VIRTUAL_ENV
    else
        builtin cd "$@"
    fi
}

cd
```
This hook simply takes you into the virtual environment project directory when you activate the environment.

Make sure so update your `~/.zshrc` or `~/.bashrc` with the following:
`export WORKON_HOME=~/Envs`
`source /usr/local/bin/virtualenvwrapper.sh`

#### Install dependencies

`workon g1-etl`

### Run webserver
FLASK_APP=server.py flask run

**REST endpoint**

http://localhost:5000/import/extract

* Expects organizationId in POST / multipart-form

### Test individual entity extraction python scripts

`$ python entities/employees 420`

* Expects organizationId pass as cmd line arg
