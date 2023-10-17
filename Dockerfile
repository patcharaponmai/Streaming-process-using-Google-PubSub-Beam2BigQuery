# Use a Jupyter Docker image as the base image
FROM jupyter/base-notebook:latest

# Set the working directory
WORKDIR /app

# Copy your source code into the container
COPY . /app

# Install Python dependencies and execute scripts
RUN pip3 install -r requirements.txt
RUN psql -h postgres -U root -d online_shopping_db -a -f notify_data_change.sql

# Expose the Jupyter Notebook port
EXPOSE 8888

# Start Jupyter Notebook
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
