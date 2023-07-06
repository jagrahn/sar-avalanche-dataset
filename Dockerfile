FROM daskdev/dask

COPY . /app
WORKDIR /app
SHELL ["/bin/bash", "--login", "-c"]

COPY ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
COPY .myopenssl.conf /root/.myopenssl.conf
ENV OPENSSL_CONF=/root/.myopenssl.conf
ENV SPATIALITE_LIBRARY_PATH=mod_spatialite
COPY .condarc /root/.condarc

ENV TZ=Europe/Oslo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update && \
    apt-get install --yes --no-install-recommends apt-utils && \
    apt-get install --yes ca-certificates openssl && \
    apt-get install --yes libgdal-dev && \
    apt-get install --yes libsqlite3-mod-spatialite && \
    apt-get install --yes screen

RUN conda install -n base -c conda-forge conda-libmamba-solver; \
    conda config --set solver libmamba; \
    conda env update -n base --file conda/docker-env.yml --prune
#    conda env create -f conda/docker-env.yml; 

ENV PYTHONPATH "${PYTHONPATH}:/app/dependencies/gdar-core:/app/dependencies/gdar-plus:/app/dependencies/gdar-geocoding"
ENV PYTHONPATH "${PYTHONPATH}:/app/dependencies/gtile:/app/dependencies/gtile/plugins/gtile-sat"

COPY ./bin/skreddata skreddata-app

#ENTRYPOINT [ "bash" ]
#ENTRYPOINT [ "python3" ]
#ENTRYPOINT [ "/opt/conda/envs/skreddata/bin/python3", "skreddata-app" ] 
