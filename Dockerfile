FROM public.ecr.aws/lambda/python:3.8 as build

# Maybe just make, gcc, and autoconf?
RUN yum groupinstall -y "Development Tools" \
    && yum install -y wget \
    && yum clean all \
    && rm -rf /var/cache/yum

# This requires cmake > 3.12 so install that
ENV CMAKE_VERSION=3.20.3
RUN wget https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-x86_64.sh \
    && sh ./cmake-${CMAKE_VERSION}-linux-x86_64.sh --prefix=/usr/local --skip-license

ENV ECCODES_VERSION=2.22.0

RUN wget https://confluence.ecmwf.int/download/attachments/45757960/eccodes-${ECCODES_VERSION}-Source.tar.gz \
    && tar xvf eccodes-${ECCODES_VERSION}-Source.tar.gz \
    && cd eccodes-${ECCODES_VERSION}-Source \
    && mkdir build \
    && cd build \
    && cmake -DENABLE_FORTRAN=false .. \
    && make \
    && make install

FROM public.ecr.aws/lambda/python:3.8

ENV ECCODES_VERSION=2.22.0

COPY --from=build /var/task/eccodes-${ECCODES_VERSION}-Source/build/lib/libeccodes.so /usr/lib/libeccodes.so
COPY --from=build /var/task/eccodes-${ECCODES_VERSION}-Source/definitions/ /usr/local/share/eccodes/definitions/

COPY requirements.txt ./

RUN pip install -r requirements.txt

COPY lambda.py ./

CMD [ "lambda.lambda_handler" ]
