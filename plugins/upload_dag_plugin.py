from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, request, jsonify
import os
from airflow.configuration import conf
import logging
from airflow.www.app import csrf  # Import Airflow's CSRF module

logger = logging.getLogger(__name__)

# Log to confirm plugin initialization
logger.info("Initializing upload_dag_plugin")

upload_dag_bp = Blueprint('upload_dag', __name__)


# Use Airflow's CSRF exemption
@upload_dag_bp.route('/api/v1/upload_dag', methods=['POST'])
@csrf.exempt  # Exempt using Airflow's CSRF handler
def upload_dag():
    logger.info("Received request to upload files")
    try:
        if 'dag_file' not in request.files:
            logger.error("No DAG file provided in request")
            return jsonify({'error': 'No DAG file provided'}), 400

        dag_file = request.files['dag_file']
        logger.info(f"Processing DAG file: {dag_file.filename}")
        if not dag_file.filename.endswith('.py'):
            logger.error("DAG file does not have .py extension")
            return jsonify({'error': 'DAG file must be a .py file'}), 400

        dag_folder = conf.get('core', 'dags_folder')
        dag_file_path = os.path.join(dag_folder, dag_file.filename)
        logger.info(f"Saving DAG file to: {dag_file_path}")
        dag_file.save(dag_file_path)

        if not os.path.exists(dag_file_path):
            logger.error(f"Failed to save DAG file: {dag_file_path}")
            return jsonify({'error': 'Failed to save DAG file'}), 500

        data_file_path = None
        if 'data_file' in request.files:
            data_file = request.files['data_file']
            data_file_path = os.path.join(dag_folder, data_file.filename)
            logger.info(f"Saving data file to: {data_file_path}")
            data_file.save(data_file_path)

            if not os.path.exists(data_file_path):
                logger.error(f"Failed to save data file: {data_file_path}")
                return jsonify({'error': 'Failed to save data file'}), 500

        logger.info(f"Files saved - DAG: {dag_file_path}, Data: {data_file_path}")
        return jsonify({
            'message': 'Files uploaded successfully',
            'dag_file_path': dag_file_path,
            'data_file_path': data_file_path
        }), 200

    except Exception as e:
        logger.error(f"Error uploading files: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@upload_dag_bp.route('/api/v1/upload_dag/test', methods=['GET'])
@csrf.exempt  # Also exempt the test endpoint
def test_endpoint():
    logger.info("Accessed test endpoint")
    return jsonify({'message': 'Upload DAG plugin is working'}), 200


class UploadDagPlugin(AirflowPlugin):
    name = 'upload_dag_plugin'
    flask_blueprints = [upload_dag_bp]