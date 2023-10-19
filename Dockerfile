# Use a Jupyter Docker image as the base image
FROM jupyter/base-notebook:latest

# Set the working directory
WORKDIR /app

# Copy your source code into the container
COPY . /app

# Install Python dependencies
RUN pip3 install -r requirements.txt

# Expose the Jupyter Notebook port
EXPOSE 8888

# Start Jupyter Notebook
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
