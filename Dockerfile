FROM daskdev/dask

COPY --from=ghcr.io/astral-sh/uv:0.9.22 /uv /uvx /bin/

WORKDIR /app
SHELL ["/bin/bash", "--login", "-c"]

ENV TZ=Europe/Oslo
ENV SPATIALITE_LIBRARY_PATH=mod_spatialite
ENV GDAL_CONFIG=/usr/bin/gdal-config
ENV UV_LINK_MODE=copy
ENV UV_COMPILE_BYTECODE=1
ENV PATH="/app/.venv/bin:${PATH}"

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update && \
    apt-get install --yes --no-install-recommends \
        build-essential \
        ca-certificates \
        gdal-bin \
        libgdal-dev \
        libspatialindex-dev \
        libsqlite3-mod-spatialite && \
    rm -rf /var/lib/apt/lists/*

COPY . /app

RUN uv sync --frozen --no-dev

CMD ["skreddata"]
