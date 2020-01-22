#ifndef WORKER_H
#define WORKER_H

#include <QObject>
#include <QDebug>
#include <QThread>
#include <QSqlDatabase>
#include <QDate>
#include <QDir>
#include <QException>
#include <QDateTime>
#include <QTextCodec>

class worker : public QObject {
    Q_OBJECT
public:
    explicit worker(QObject *parent = 0);
    ~worker();
    void myconnect(QSqlDatabase &db1, QString basename, QString zkhisserver_host, QString zkhisserver_user, QString zkhisserver_password, QString database);
    void getconfig(int i, QString currentpath, QString user, QString pwd, QString hostname, QString port, QString change);
    void readtratable(QString initpath, int trade_count, QVector<QString>& TradeList, QVector<QString>& FieldList,QString tabname, QString fieldname, QString year, QString month);
    void bcpout(QString tabname, QString fieldname, QString cmd2, float progress);
    void bcpin(QString tabname, float progress, QString cmdin);
    void deletetable(QString tabname, QString fieldname, QSqlQuery zkqueryrun,QSqlDatabase db1);
    void datacheck(QString tabname, QString fieldname,QSqlQuery firstqueryrun,QSqlQuery zkqueryrun, QString cmd, QString cmdin, QString sql_orderrec, QSqlDatabase db1);
    void EncryPassWord(QString iniPath, int i, QString pwd, QString change);
    void DecryPassWord(int i);
    QString byteToQString(const QByteArray &byte);
    QByteArray qstringToByte(const QString &strInfo);
    QString XorEncryptDecrypt(const QString &, const char &);


signals:
    void showNum(int,float,int,QString,QString);
    void showDiff(QString,QString,int,int,int);
    void updatesignal();
    void exceptionsignal(QString);

public slots:
    //线程处理函数
    void DeDabaFirst();
    void DeDabaSecond();
    void DeDabaThird();
    void DeDabaFourth();
    void DeDabaFifth();
    void DeDabaSixth();

private:

public:
 //   QSqlDatabase db2,db3,db4,db5,db6,db7;
    QString dt;
    QString hisserver_user[7]       = {"","","","","","",""};
    QString hisserver_password[7]   = {"","","","","","",""};
    QString hisserver_ip[7]         = {"","","","","","",""};
    QString hisserver_port[7]       = {"","","","","","",""};
    QString hisserver_host[7]       = {"","","","","","",""};
    int changepass[7]               = {1,1,1,1,1,1,1};

    int trade_count                 = 0;
    int his_count                   = 0 ;
};


#endif // WORKER_H
