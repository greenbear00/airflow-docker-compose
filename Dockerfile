FROM apache/airflow:2.2.2

RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip3 install pymysql
RUN pip3 install plyvel

########################################################################
# root 권한으로 ps, vi 설치 
# - apply alias ll
########################################################################
USER root
RUN apt-get update && apt-get install -y procps vim
RUN echo "alias ll='ls --color=auto -alF'" >> ~/.bashrc
RUN source ~/.bashrc

########################################################################
# by airflow user
# - install jupyter (-> $ jupyter notebook list하면 기본 airflow로 나옴)
# - apply alias ll
########################################################################
USER airflow

RUN pip3 install -U jupyter-core 
RUN pip3 install -U jupyter 
RUN chmod -R 775 /home/airflow/.local/share/jupyter

RUN echo "alias ll='ls --color=auto -alF'" >> ~/.bashrc
RUN source ~/.bashrc

WORKDIR /opt/airflow/script



########################################################################
# default user
# - apply alias ll
########################################################################
USER ${AIRFLOW_UID}
RUN echo "alias ll='ls --color=auto -alF'" >> ~/.bashrc
RUN source ~/.bashrc
WORKDIR /opt/airflow



ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
CMD []

