FROM nrel/openstudio:2.9.1 as base

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8

ARG TZ=America/Los_Angeles
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN sudo apt update && \
    sudo apt install -y wget build-essential checkinstall libreadline-gplv2-dev libncursesw5-dev libssl-dev \
    libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev libffi-dev zlib1g-dev

RUN wget https://www.python.org/ftp/python/3.8.8/Python-3.8.8.tgz && \
    tar xzf Python-3.8.8.tgz && \
    cd Python-3.8.8 && \
    ./configure --enable-optimizations && \
    make altinstall && \
    rm -rf Python-3.8.8 && \
    rm -rf Python-3.8.8.tgz

RUN python3.8 -m pip install pyarrow

COPY . /buildstock-batch/
RUN python3.8 -m pip install /buildstock-batch

FROM base as custom-gems
RUN sudo cp /buildstock-batch/Gemfile /var/simdata/
RUN bundle config path /var/simdata/.custom_gems/
RUN bundle config without 'test development'
RUN bundle install --gemfile /var/simdata/Gemfile