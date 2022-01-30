FROM ubuntu:16.04
MAINTAINER J. Dumont <j.dumont@coinamics.io>
RUN apt-get update && apt-get install -y libssl-dev pkg-config ca-certificates
ENV appname server-ohlcsaver




RUN mkdir -p /coinfolio && mkdir /coinfolio/${appname}
ADD target/release/server-ohlcsaver /coinfolio/${appname}
RUN chmod 777 /coinfolio/${appname}/server-ohlcsaver &&  ulimit -n 2048
CMD exec /coinfolio/${appname}/server-ohlcsaver ${pairs}