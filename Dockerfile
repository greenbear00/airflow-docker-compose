FROM apache/airflow:2.3.2

RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip3 install pymysql pymssql plyvel

########################################################################
# root 권한으로 ps, vi 설치 
# - apply alias ll
########################################################################
USER root

RUN echo "airflow_uid = ${AIRFLOW_UID}"
RUN apt-get update && apt-get install -y procps vim
RUN echo "alias ll='ls --color=auto -alF'" >> ~/.bashrc
RUN source ~/.bashrc

# sudo 권한 적용함과 동시에 최초 수행시에도 암호를 입력하지 않음
RUN adduser airflow sudo
RUN echo 'airflow ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers.d/airflow
# RUN adduser ${AIRFLOW_UID} sudo
RUN echo 'default ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers.d/default
# password 없이 su 명령어 가능
RUN sed -i 's/# auth       sufficient pam_wheel.so trus/ auth       sufficient pam_wheel.so trus/g' /etc/pam.d/su


########################################################################
# by airflow user
# - install jupyter (-> $ jupyter notebook list하면 기본 airflow로 나옴)
# - apply alias ll
########################################################################
USER airflow

# 참고: https://xnuinside.medium.com/install-python-dependencies-to-docker-compose-cluster-without-re-build-images-8c63a431e11c
RUN mkdir /opt/airflow/packages
COPY ./packages.pth /home/airflow/.local/lib/python3.7/site-packages
RUN sudo chmod -R o+rwx /home/airflow/.local/lib/python3.7/site-packages

RUN pip3 install -U jupyter-core 
RUN pip3 install -U jupyter 
RUN chmod -R 775 /home/airflow/.local/share/jupyter

RUN echo "alias ll='ls --color=auto -alF'" >> ~/.bashrc
RUN source ~/.bashrc

WORKDIR /opt/airflow/script



########################################################################
# airflow user
# - apply alias ll
# in host terminal
# $ export AIRFLOW_UID=${id -u}
########################################################################
USER ${AIRFLOW_UID}
RUN echo "airflow_uid = ${AIRFLOW_UID}"
RUN echo "alias ll='ls --color=auto -alF'" >> ~/.bashrc
RUN source ~/.bashrc
WORKDIR /opt/airflow



ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
CMD []

