# FROM --platform=linux/amd64 mono:latest
FROM mcr.microsoft.com/dotnet/runtime:7.0

RUN apt-get update && \
    apt-get install -y \
        python3 \
        python3-pip \
        build-essential \
        cmake \
        dh-autoreconf
RUN pip3 install boto3
RUN apt-get install unzip
RUN mkdir /var/task
WORKDIR /var/task

ADD bin/Release/net7.0/archive.zip archive.zip
RUN unzip archive.zip -d /var/task/thermo
ENTRYPOINT ["dotnet", "/var/task/thermo/ThermoParquet.dll"]
