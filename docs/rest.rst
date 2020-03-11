rest
=======
Prototype `OpenAPI`_ server based on `FastAPI`_.

Configure `DIRECTORIES` in `.env` file or environment variable

   % uvicorn lupyne.services.rest:app [--reload]

Open http://localhost:8000/docs

.. _OpenAPI: https://github.com/OAI/OpenAPI-Specification
.. _FastAPI: https://fastapi.tiangolo.com
