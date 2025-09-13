FROM quay.io/astronomer/astro-runtime:12.4.0

USER root

# Install OpenJDK-17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Find the actual Java path and set JAVA_HOME
RUN java_path=$(dirname $(dirname $(readlink -f $(which java)))) && \
    echo "Java found at: $java_path" && \
    echo "export JAVA_HOME=$java_path" >> /etc/profile.d/java.sh && \
    echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> /etc/profile.d/java.sh

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

# Verify Java installation
RUN java -version && javac -version

USER astro