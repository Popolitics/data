FROM kestra/kestra:v0.19.0

WORKDIR /app

COPY --chown=kestra:kestra flows/ /app/flows/
COPY --chown=kestra:kestra src/ /app/src/
COPY --chown=kestra:kestra job/ /app/job/
COPY --chown=kestra:kestra lib/ /app/lib/

USER kestra
