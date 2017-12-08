# g1-etl
GrowOne MMJ ETL

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
