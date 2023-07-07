FROM daskdev/dask

COPY . /app
WORKDIR /app
SHELL ["/bin/bash", "--login", "-c"]

# SSL certificate setup: 
COPY ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
COPY .myopenssl.conf /root/.myopenssl.conf
ENV OPENSSL_CONF=/root/.myopenssl.conf
ENV SPATIALITE_LIBRARY_PATH=mod_spatialite

# Time zone: 
ENV TZ=Europe/Oslo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Apps: 
RUN apt-get update && \
    apt-get install --yes --no-install-recommends apt-utils && \
    apt-get install --yes ca-certificates openssl && \
    apt-get install --yes libgdal-dev && \
    apt-get install --yes libsqlite3-mod-spatialite && \
    apt-get install --yes screen

# Conda: 
RUN conda config --set ssl_verify /etc/ssl/certs/ca-certificates.crt; \
    conda config --set channels conda-forge; \
    conda install -n base conda-libmamba-solver; \
    conda config --set solver libmamba; \
    conda env update -n base --file conda/docker-env.yml --prune
#    conda env create -f conda/docker-env.yml; 

# Local python dependencies: 
ENV PYTHONPATH "${PYTHONPATH}:/app/dependencies/gdar-core:/app/dependencies/gdar-plus:/app/dependencies/gdar-geocoding"
ENV PYTHONPATH "${PYTHONPATH}:/app/dependencies/gtile:/app/dependencies/gtile/plugins/gtile-sat"

# Entry point: 
#ENTRYPOINT [ "bash" ]
#ENTRYPOINT [ "python3" ]
ENTRYPOINT [ "python3", "/app/bin/skreddata" ] 
