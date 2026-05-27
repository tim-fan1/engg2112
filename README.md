Fuel price prediction by postcode in Greater Sydney, using historical fuel prices, crude oil prices, AUD-USD exchange rates, Terminal Gate Price indices, local Sydney BOM weather. 

# Model

## Preprocessing

Run all ``preprocess.ipynb``. It assumes the various datasets in ``datasets/`` have been downloaded and are ready for consolidation. The script consolidates those various datasets into a single ``MODEL_READY_DATASET`` csv file.

## Training

Run all ``training.ipynb``. It assumes preprocessing has been completed, though will throw an error if it doesn't detect a ``MODEL_READY_DATASET`` csv file. Note that this script runs two different training pipelines in series. Specifically, one using a random train/test split, and one using a chronological train/test split. This was to compare the results from the two kinds of train/test splits.

## Exporting

The scikit-learn model can be exported for Python usage elsewhere by using joblib. This is done by the provided app in ``app/backend/export_rf_model.py`` which can be referred to for model export instructions.

# App

The frontend is just a single ``index.html`` file with a plaintext file listing all NSW suburbs in a grid topology. So a frontend server will be required to serve a user this plaintext file. 

For development, an extension like the Live Server for Visual Studio Code can be used. For deployment, the application can be hosted as a static site using services such as GitHub Pages, or any standard web server such as an Nginx HTTP Server.

The frontend server should serve the files in ``app/frontend/``. For development and testing, an extension like the ***Live Server for Visual Studio Code*** extension should be used.

The backend is a simple Python Flask server at ``app.py`` that uses the previously exported scikit-learn ML model to serve price prediction requests from the frontend. For development and deployment a web server with Python installed should be sufficient, with other necessary requirements installable through ``pip install -r requirements.txt``. 

The backend Flask server should be run by ``python app/backend/app.py``.

