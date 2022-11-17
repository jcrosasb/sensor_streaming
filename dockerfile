FROM continuumio/miniconda3:4.10.3p1

WORKDIR /project

# Create the environment:
COPY env.yml .
RUN conda env create -f env.yml

# Copy directories
COPY ./README.md /project
COPY ./notebooks /project/notebooks
COPY ./data /project/data
COPY ./scripts /project/scripts

# Make RUN commands use the new environment:
SHELL ["conda", "run", "-n", "myenv", "/bin/bash", "-c"]

# Demonstrate the environment is activated:
RUN echo "Make sure pandas is installed:"
RUN python -c "import pandas"

ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "myenv", "jupyter-lab","--ip=0.0.0.0","--no-browser","--allow-root"]
