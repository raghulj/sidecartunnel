# The reference application. An ordinary WSGI container: no async worker class, no
# monkey-patching, no websocket library.
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./

EXPOSE 5000

# A fixed worker pool, which is the premise of the design. Four workers is also why a
# nonce cache must live in Redis rather than in a process-local dict.
#
# --access-logfile - puts the request log on stdout, where `docker compose logs webapp`
# finds it. Without it the connect webhook fires invisibly and there is nothing to grep
# when a connection is being refused.
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:5000", \
     "--access-logfile", "-", "app:create_app()"]
