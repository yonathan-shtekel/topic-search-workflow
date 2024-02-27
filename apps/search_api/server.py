import asyncio
from flask_cors import CORS
from flask import Flask, Blueprint, current_app, jsonify, request, g
from temporalio.client import Client
from config import settings
from src.kafka.entites import KafkaQuery
from src.repository.redis_repository import RedisRepository
from src.workflow.activities import TASK_QUEUE_NAME
from src.workflow.search_workflow import KafkaSearchWorkflow

bp = Blueprint('search', __name__, url_prefix='/api/v1/kafka-search')


def configure_logging():
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log = logging.getLogger(__name__)
    return log


logger = configure_logging()


def create_app() -> Flask:
    application = Flask(__name__)
    application.config['REDIS_CONFIG'] = {
        'host': settings.redis_host,
        'port': settings.redis_port
    }
    application.register_blueprint(bp)
    CORS(application)

    @application.teardown_appcontext
    def close_redis_client(error=None):

        if error:
            logger.error(f"Error: {error}")

        redis_client = g.pop('redis_client', None)
        if redis_client is not None:
            redis_client.client.close()

    return application


async def connect_temporal(application: Flask):
    client = await Client.connect(settings.temporal_url)
    application.temporal_client = client


def get_client() -> Client:
    return current_app.temporal_client


def get_redis_client() -> RedisRepository:
    if 'redis_client' not in g:
        g.redis_client = RedisRepository(rc=current_app.config['REDIS_CONFIG'])
    return g.redis_client


@bp.route("/start", methods=["POST"])
async def start():
    client = get_client()
    redis_client = get_redis_client()

    try:
        data = request.get_json()
        query = KafkaQuery(**data)

        if redis_client.exists(query.id):
            return jsonify({"id": query.id}), 200

        workflow_id = f"topic-search-workflow-{query.id}"

        logger.info(f"Executing workflow {workflow_id}")

        await client.start_workflow(
            KafkaSearchWorkflow.run,
            query,
            id=workflow_id,
            task_queue=TASK_QUEUE_NAME,
        )

        logger.info(f"Workflow {workflow_id} executed")

        redis_client.search_started(query.id)

        return jsonify({"id": query.id}), 202
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@bp.route("/results/<search_id>", methods=["GET"])
def search_results(search_id: str):
    redis_client = get_redis_client()
    results = redis_client.get_list(search_id)
    return jsonify({"results": results}), 200


if __name__ == "__main__":
    app = create_app()
    # Create Temporal connection and start API in an async manner
    asyncio.run(connect_temporal(app))
    app.run(debug=True, port=5001)
