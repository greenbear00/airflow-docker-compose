FROM apache/airflow:2.3.2

RUN /usr/local/bin/python -m pip install --upgrade pip --proxy http://192.168.1.139:3128
RUN pip3 install pymysql --proxy http://192.168.1.139:3128
RUN pip3 install plyvel --proxy http://192.168.1.139:3128

########################################################################
# root 권한으로 ps, vi 설치 
# - apply alias ll
# - append airflow script
########################################################################
USER root
# apt-get에 proxy
RUN export http_proxy="http://192.168.1.139:3128"
RUN export https_proxy="http://192.168.1.139:3128"
RUN touch /etc/apt/apt.conf.d/proxy.conf 
RUN echo "Acquire::http::Proxy \"http://192.168.1.139:3128\";" > /etc/apt/apt.conf.d/proxy.conf
RUN echo "Acquire::https::Proxy \"http://192.168.1.139:3128\";" >> /etc/apt/apt.conf.d/proxy.conf
RUN cat /etc/apt/apt.conf.d/proxy.conf


RUN apt-get update && apt-get install -y procps vim
RUN echo "alias ll='ls --color=auto -alF'" >> ~/.bashrc
RUN source ~/.bashrc
# # airflow에 해당하는 script 추가 및 권한 적용
# WORKDIR /opt/airflow/script
# ADD airflow/* /opt/airflow/script
# RUN chown airflow:root *.sh
# RUN chmod u+x *.sh

########################################################################
# by airflow user
# - install jupyter
# - apply alias ll
########################################################################
USER airflow
RUN pip3 install -U jupyter-core --proxy http://192.168.1.139:3128
RUN pip3 install -U jupyter --proxy http://192.168.1.139:3128
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

