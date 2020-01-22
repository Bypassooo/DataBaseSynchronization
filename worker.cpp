#include "worker.h"
#include <QDebug>
#include <QThread>
#include <QSettings>
#include <QMessageBox>
#include <QSqlError>
#include <QApplication>
#include <QSqlQuery>
#include <QProcess>
#include <windows.h>
#include <QFile>

worker::worker(QObject *parent) : QObject(parent)
{

}
QString worker::XorEncryptDecrypt(const QString &str, const char &key)
{
    QString result;
    QByteArray bs = this->qstringToByte(str);
    int i;
    for(i=0; i<bs.size(); i++)
    {
      bs[i] = bs[i] ^ key;
    }
    result = byteToQString(bs);
    return result;
}
QString worker::byteToQString(const QByteArray &byte)
{
    QString result;
    if(byte.size() > 0)
    {
      QTextCodec *codec = QTextCodec::codecForName("utf-8");
      result = codec->toUnicode(byte);
    }
    return result;
}
QByteArray worker::qstringToByte(const QString &strInfo)
{
    QByteArray result;
    if(strInfo.length() > 0)
    {
      QTextCodec *codec = QTextCodec::codecForName("utf-8");
      result = codec->fromUnicode(strInfo);
    }
    return result;
}
void worker::EncryPassWord(QString iniPath, int i, QString passa, QString changeb)
{
    QSettings settings_setpwd(iniPath,QSettings::IniFormat);
    QString pwd = XorEncryptDecrypt(hisserver_password[i],12);
    settings_setpwd.setValue(passa,pwd);
    settings_setpwd.setValue(changeb,0);
}
void worker::DecryPassWord(int i)
{
    hisserver_password[i] = XorEncryptDecrypt(hisserver_password[i],12);
}
void worker::myconnect(QSqlDatabase &db1, QString basename, QString zkhisserver_host, QString zkhisserver_user, QString zkhisserver_password, QString database)
{
    db1=QSqlDatabase::addDatabase("QODBC",basename);
    db1.setDatabaseName(QString("DRIVER={SQL SERVER};"
                                   "SERVER=%1;" //服务器名称
                                   "DATABASE=%2;"//数据库名
                                   "UID=%3;"           //登录名
                                   "PWD=%4;"        //密码
                                   ).arg(zkhisserver_host)
                           .arg(database)
                           .arg(zkhisserver_user)
                           .arg(zkhisserver_password)
                           );
    if (!db1.open())
    {
        qDebug()<<db1.lastError().databaseText();
    }
    else
    {
        qDebug()<<"success";
    }
}
void worker::getconfig(int i,QString initpath,QString user, QString pwd, QString hostname, QString port, QString change)
{
    QSettings settings_init(initpath,QSettings::IniFormat);
    hisserver_user[i] = settings_init.value(user).toString();
    hisserver_password[i] = settings_init.value(pwd).toString();
    hisserver_ip[i] = settings_init.value(hostname).toString();
    hisserver_port[i] = settings_init.value(port).toString();
    changepass[i] = settings_init.value(change).toInt();
    hisserver_host[i] = hisserver_ip[i].append(",").append(hisserver_port[i]);
    trade_count = settings_init.value("/trade/count",0).toInt();
    his_count = settings_init.value("/history/count",0).toInt();
    if(changepass[i]  == 1)
        EncryPassWord(initpath,i,pwd,change);
    else
        DecryPassWord(i);
}
void worker::readtratable(QString initpath, int trade_count, QVector<QString>& TradeList, QVector<QString>& FieldList,QString tabname, QString fieldname, QString year, QString month)
{
    QSettings settings_init(initpath,QSettings::IniFormat);
    //  读取配置文件，将需要同步的当前表存在TradeList中,时间字段存在FieldList中
    for(int i =1;i < trade_count+1; i++)
    {
        //拼接tab
        QString tab = tabname;
        QString field = fieldname;
        tab.append(QString::number(i, 10));
        field.append(QString::number(i, 10));
        //获取表名
        QString TableName = settings_init.value(tab,0).toString();
        QString DateField = settings_init.value(field,0).toString();
        TableName.replace("%A",year).replace("%B",month);
        TradeList.append(TableName);
        FieldList.append(DateField);
    }
}
void worker::DeDabaFirst()
{
    try {
        QVector<QString> TradeList;
        QVector<QString> HisList;
        QVector<QString> FieldList;
        QVector<QString> HisFieldList;

        QString year = this->dt.mid(0,4);
        QString month = this->dt.mid(4,2);

        QString Currentpath = QDir::currentPath();
        QString TodayDirpath = Currentpath;
        TodayDirpath.append("/").append(this->dt);
        QString Server1Path = TodayDirpath;
        Server1Path.append("/server1");
        QDir dir(TodayDirpath);
        QDir dirserver1(Server1Path);

        QString Firstlog = Currentpath;
        Firstlog.append("/log/");
        QDir dirlog(Firstlog);
        if(!dirlog.exists())
        {
            dirlog.mkdir(Firstlog);//创建log目录
        }
        Firstlog.append(this->dt);
        QDir dirlogday(Firstlog);
        if(!dirlogday.exists())
        {
            dirlogday.mkdir(Firstlog);//创建log/日期目录
        }
        Firstlog.append("/server1.log");
        QFile file(Firstlog);
        QString logmessage;

        if(!dir.exists())
        {
            dir.mkdir(TodayDirpath);//只创建一级子目录，即必须保证上级目录存在
        }
        if(!dirserver1.exists())
        {
            dirserver1.mkdir(Server1Path);
        }
        else if(!dirserver1.isEmpty())
        {
            //先删除目录，再创建目录
            dirserver1.removeRecursively();
            dirserver1.mkdir(Server1Path);
        }
        //读取配置信息
        QString iniPath = Currentpath.append("/config.ini");
        getconfig(1,iniPath, "/firsthisserver/user", "/firsthisserver/password", "/firsthisserver/hostname", "/firsthisserver/port", "/firsthisserver/changepassword");
        getconfig(0,iniPath, "/zkhisserver/user", "/zkhisserver/password", "/zkhisserver/hostname", "/zkhisserver/port","/zkhisserver/changepassword");

        readtratable(iniPath,trade_count, TradeList, FieldList,"/trade/tab","/trade/field",year,month);
        readtratable(iniPath,his_count, HisList, HisFieldList,"/history/tab","/history/field",year,month);

        int progress_count = 0; //已经复制的表的数量
        float progress = 1.00; //进度条数据
        QDateTime * starttime1 = new QDateTime();
        QString begintime;

        //进行数据核对 db1是总控历史run库   db2是总控历史his库  db3是节点一历史run库   db4是节点一历史his库
        QSqlDatabase db1;
        QSqlDatabase db2;
        QSqlDatabase db3;
        QSqlDatabase db4;
        myconnect(db1, "zkrunfirst", hisserver_host[0], hisserver_user[0], hisserver_password[0], "run");
        myconnect(db2, "zkhisfirst", hisserver_host[0], hisserver_user[0], hisserver_password[0], "his");
        myconnect(db3, "firstrun", hisserver_host[1], hisserver_user[1], hisserver_password[1], "run");
        myconnect(db4, "firsthis", hisserver_host[1], hisserver_user[1], hisserver_password[1], "his");
        QSqlQuery zkqueryrun(db1);
        QSqlQuery zkqueryhis(db2);
        QSqlQuery firstqueryrun(db3);
        QSqlQuery firstqueryhis(db4);

        for(int i=0; i<trade_count; i++)
        {
            progress_count++;
            progress = (progress_count/float((trade_count+his_count)*2))*100;
            QString tabname = TradeList[i];
            QString fieldname = FieldList[i];
            QString sql = "select * from run..";
            sql.append(tabname);
            sql.append(" where serverid = '1'");
            if(fieldname != "-1")
            {
                sql.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            QProcess process(this);
            QString cmd = "bcp \"";
            // bcp run..uncommitclear in uncommitclear.bcp   -S
            QString cmdin = "bcp run..";
            cmdin.append(tabname);
            cmdin.append(" in ");
            cmdin.append(Server1Path);
            cmdin.append("\\");
            cmdin.append(tabname);
            cmdin.append(".bcp  -S");
            cmdin.append(hisserver_ip[0]);
            cmdin.append(",");
            cmdin.append(hisserver_port[0]);
            cmdin.append(" -U");
            cmdin.append(hisserver_user[0]);
            cmdin.append(" -P");
            cmdin.append(hisserver_password[0]);
            cmdin.append(" -n");
            cmd.append(sql).append("\"").append(" queryout ").append(Server1Path).append("\\").append(tabname).append(".bcp  -S").append(hisserver_ip[1]).append(",").append(hisserver_port[1]).append(" -U").append(hisserver_user[1]).append(" -P").append(hisserver_password[1]).append(" -n");
            begintime = starttime1->currentDateTime().toString("yyyy.MM.dd hh:mm:ss");
            process.start(cmd);
            file.open(QIODevice::WriteOnly | QIODevice::Append);
            QTextStream text_stream1(&file);
            logmessage = "server1:";
            logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append("bcp out table ").append(tabname).append(":").append(sql);
            text_stream1 << logmessage << "\r\n";
            file.flush();
            file.close();
            emit showNum(1,progress,1,tabname,begintime);//触发信号
            process.waitForFinished(-1);

            QString sql_orderrec="delete  from run..";
            sql_orderrec.append(tabname);
            sql_orderrec.append(" where serverid = '1'");
            if(fieldname != "-1")
            {
                sql_orderrec.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            bool result = false;
            while(!result)
            {
                result = zkqueryrun.exec(sql_orderrec);
            }
                if(result)
                {
                    db1.commit();
                    file.open(QIODevice::WriteOnly | QIODevice::Append);
                    QTextStream text_stream2(&file);
                    logmessage = "server1:";
                    logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append(sql_orderrec);
                    text_stream2 << logmessage << "\r\n";
                    file.flush();
                    file.close();

                    progress_count++;
                    progress = (progress_count/float((trade_count+his_count)*2))*100;
                    QProcess processin(this);
                    begintime = starttime1->currentDateTime().toString("yyyy.MM.dd hh:mm:ss");
                    processin.start(cmdin);
                    file.open(QIODevice::WriteOnly | QIODevice::Append);
                    QTextStream text_stream4(&file);
                    logmessage = "server1:";
                    logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append("bcp in ").append(tabname);
                    text_stream4 << logmessage << "\r\n";
                    file.flush();
                    file.close();
                    emit showNum(1,progress,2,tabname,begintime);//触发信号
                    processin.waitForFinished(-1);
                }
                else //删除表失败时不进行bcp in
                {
                    db1.rollback();
                    QString me = "节点一删除表";
                    me.append(tabname).append("失败").append(zkqueryrun.lastError().databaseText());
                    emit exceptionsignal(me);
                    file.open(QIODevice::WriteOnly | QIODevice::Append);
                    QTextStream text_stream3(&file);
                    logmessage = "server1:";
                    logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":回滚:").append(sql_orderrec).append(me);
                    text_stream3 << logmessage << "\r\n";
                    file.flush();
                    file.close();
                }

            //进行数据核对
            int firstruncount = 0;
            int zkruncount = 0;
            QString sql_firstruncheck="select count(*)  from run..";
            sql_firstruncheck.append(tabname);
            sql_firstruncheck.append(" WITH(NOLOCK) where serverid = '1'");
            if(fieldname != "-1")
            {
                sql_firstruncheck.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            firstqueryrun.exec(sql_firstruncheck);
            if(firstqueryrun.next())
            {
                firstruncount = firstqueryrun.value(0).toInt();
            }
            zkqueryrun.exec(sql_firstruncheck);
            if(zkqueryrun.next())
            {
                zkruncount = zkqueryrun.value(0).toInt();
            }
            file.open(QIODevice::WriteOnly | QIODevice::Append);
            QTextStream text_stream5(&file);

            logmessage = "server1:";
            logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("数据核对:").append(sql_firstruncheck);
            logmessage.append(" server1-").append(tabname).append("=").append(QString::number(firstruncount,10));
            logmessage.append(" zk-server1-").append(tabname).append("=").append(QString::number(zkruncount,10));
            text_stream5 << logmessage << "\r\n";
            file.flush();
            file.close();

            //第一次核对不平，重新执行bcp
            int checkcount = 0;
            while(checkcount < 5 && firstruncount != zkruncount)
            {
                checkcount++;
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream6(&file);
                logmessage = "server1:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("数据核对不平:").append(sql_firstruncheck);
                logmessage.append(" server1-").append(tabname).append("=").append(QString::number(firstruncount,10));
                logmessage.append(" zk-server1-").append(tabname).append("=").append(QString::number(zkruncount,10));
                text_stream6 << logmessage << "\r\n";
                logmessage = "开始下一遍bcp...";
                text_stream6 << logmessage << "\r\n";
                file.flush();
                file.close();

                //删除bcpout文件
                QString removepath = Server1Path;
                removepath.append("\\").append(tabname).append(".bcp");
                QFile::remove(removepath);
                QProcess processagain(this);
                processagain.start(cmd);
                processagain.waitForFinished(-1);
                bool result = false;
                while(!result)
                {
                    result = zkqueryrun.exec(sql_orderrec);
                }
                if(result)
                {
                    db1.commit();
                    QProcess processinagain(this);
                    processinagain.start(cmdin);
                    processinagain.waitForFinished(-1);
                }
                else
                {
                    db1.rollback();
                }
                firstqueryrun.exec(sql_firstruncheck);
                if(firstqueryrun.next())
                {
                    firstruncount = firstqueryrun.value(0).toInt();
                }
                zkqueryrun.exec(sql_firstruncheck);
                if(zkqueryrun.next())
                {
                    zkruncount = zkqueryrun.value(0).toInt();
                }
            }



            if(firstruncount != zkruncount)
            {
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream7(&file);
                logmessage = "server1:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("请人工处理不平数据:").append(sql_firstruncheck);
                logmessage.append(" server1-").append(tabname).append("=").append(QString::number(firstruncount,10));
                logmessage.append(" zk-server1-").append(tabname).append("=").append(QString::number(zkruncount,10));
                text_stream7 << logmessage << "\r\n";
                file.flush();
                file.close();
                emit showDiff("节点一",tabname,firstruncount,zkruncount,firstruncount-zkruncount);
            }

        }

        for(int k=0; k<his_count; k++)
        {
            progress_count++;
            progress = (progress_count/float((trade_count+his_count)*2))*100;
            QString tabname = HisList[k];
            QString fieldname = HisFieldList[k];
            QString sql = "select * from his..";
            sql.append(tabname);
            sql.append(" where serverid = '1'");
            if(fieldname != "-1")
            {
                sql.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            QProcess processhis(this);
            QString cmd = "bcp \"";
            cmd.append(sql);
            cmd.append("\"");
            cmd.append(" queryout ");
            cmd.append(Server1Path);
            cmd.append("\\");
            cmd.append(tabname);
            cmd.append(".bcp  -S");
            cmd.append(hisserver_ip[1]);
            cmd.append(",");
            cmd.append(hisserver_port[1]);
            cmd.append(" -U");
            cmd.append(hisserver_user[1]);
            cmd.append(" -P");
            cmd.append(hisserver_password[1]);
            cmd.append(" -n");
            QString cmdin = "bcp his..";
            cmdin.append(tabname);
            cmdin.append(" in ");
            cmdin.append(Server1Path);
            cmdin.append("\\");
            cmdin.append(tabname);
            cmdin.append(".bcp  -S");
            cmdin.append(hisserver_ip[0]);
            cmdin.append(",");
            cmdin.append(hisserver_port[0]);
            cmdin.append(" -U");
            cmdin.append(hisserver_user[0]);
            cmdin.append(" -P");
            cmdin.append(hisserver_password[0]);
            cmdin.append(" -n");
            begintime = starttime1->currentDateTime().toString("yyyy.MM.dd hh:mm:ss");
            processhis.start(cmd);
            file.open(QIODevice::WriteOnly | QIODevice::Append);
            QTextStream text_stream8(&file);
            logmessage = "server1:";
            logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append("bcp out table ").append(tabname).append(":").append(sql);
            text_stream8 << logmessage << "\r\n";
            file.flush();
            file.close();
            emit showNum(1,progress,1,tabname,begintime);//触发信号
            processhis.waitForFinished(-1);

            QString sql_orderrec="delete  from his..";
            sql_orderrec.append(tabname);
            sql_orderrec.append(" where serverid = '1'");
            if(fieldname != "-1")
            {
                sql_orderrec.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            bool result = false;
            while(!result)
            {
                result = zkqueryhis.exec(sql_orderrec);
            }
            if(result)
            {
                db2.commit();
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream9(&file);
                logmessage = "server1:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append(sql_orderrec);
                text_stream9 << logmessage << "\r\n";
                file.flush();
                file.close();

                progress_count++;
                progress = (progress_count/float((trade_count+his_count)*2))*100;
                QProcess processin(this);
                begintime = starttime1->currentDateTime().toString("yyyy.MM.dd hh:mm:ss");
                processin.start(cmdin);
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream11(&file);
                logmessage = "server1:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append("bcp in ").append(tabname);
                text_stream11 << logmessage << "\r\n";
                file.flush();
                file.close();
                emit showNum(1,progress,2,tabname,begintime);//触发信号
                processin.waitForFinished(-1);
            }
            else
            {
                db2.rollback();
                QString me = "节点一删除表";
                me.append(tabname).append("失败").append(zkqueryhis.lastError().databaseText());
                emit exceptionsignal(me);
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream10(&file);
                logmessage = "server1:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":回滚:").append(sql_orderrec).append(me);
                text_stream10 << logmessage << "\r\n";
                file.flush();
                file.close();
            }


            //进行数据核对
            int zkhiscount = 0;
            int firsthiscount = 0;
            QString sql_firsthischeck="select count(*)  from his..";
            sql_firsthischeck.append(tabname);
            sql_firsthischeck.append(" WITH(NOLOCK) where serverid = '1'");
            if(fieldname != "-1")
            {
                sql_firsthischeck.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            firstqueryhis.exec(sql_firsthischeck);
            if(firstqueryhis.next())
            {
                firsthiscount = firstqueryhis.value(0).toInt();
            }

            zkqueryhis.exec(sql_firsthischeck);
            if(zkqueryhis.next())
            {
                zkhiscount = zkqueryhis.value(0).toInt();
            }
            file.open(QIODevice::WriteOnly | QIODevice::Append);
            QTextStream text_stream12(&file);
            logmessage = "server1:";
            logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("数据核对:").append(sql_firsthischeck);
            logmessage.append(" server1-").append(tabname).append("=").append(QString::number(firsthiscount,10));
            logmessage.append(" zk-server1-").append(tabname).append("=").append(QString::number(zkhiscount,10));
            text_stream12 << logmessage << "\r\n";
            file.flush();
            file.close();

            //第一次核对不平，重新执行bcp
            int checkcount = 0;
            while(checkcount < 5 && firsthiscount != zkhiscount)
            {
                checkcount++;
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream13(&file);
                logmessage = "server1:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("数据核对不平:").append(sql_firsthischeck);
                logmessage.append(" server1-").append(tabname).append("=").append(QString::number(firsthiscount,10));
                logmessage.append(" zk-server1-").append(tabname).append("=").append(QString::number(zkhiscount,10));
                text_stream13 << logmessage << "\r\n";
                logmessage = "开始下一遍bcp...";
                text_stream13 << logmessage << "\r\n";
                file.flush();
                file.close();

                //删除bcpout文件
                QString removepath = Server1Path;
                removepath.append("\\").append(tabname).append(".bcp");
                QFile::remove(removepath);
                QProcess processagain(this);
                processagain.start(cmd);
                processagain.waitForFinished(-1);
                bool result = false;
                while(!result)
                {
                    result = zkqueryhis.exec(sql_orderrec);
                }
                if(result)
                {
                    db1.commit();
                    QProcess processinagain(this);
                    processinagain.start(cmdin);
                    processinagain.waitForFinished(-1);
                }
                else
                {
                    db1.rollback();
                }

                firstqueryhis.exec(sql_firsthischeck);
                if(firstqueryhis.next())
                {
                    firsthiscount = firstqueryhis.value(0).toInt();
                }
                zkqueryhis.exec(sql_firsthischeck);
                if(zkqueryhis.next())
                {
                    zkhiscount = zkqueryhis.value(0).toInt();
                }
            }
            if(firsthiscount != zkhiscount)
            {
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream14(&file);
                logmessage = "server1:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("请人工处理不平数据:").append(sql_firsthischeck);
                logmessage.append(" server1-").append(tabname).append("=").append(QString::number(firsthiscount,10));
                logmessage.append(" zk-server1-").append(tabname).append("=").append(QString::number(zkhiscount,10));
                text_stream14 << logmessage << "\r\n";
                file.flush();
                file.close();
                emit showDiff("节点一",tabname,firsthiscount,zkhiscount,firsthiscount-zkhiscount);
            }
        }
        db1.close();
        db2.close();
        db3.close();
        db4.close();

        file.open(QIODevice::WriteOnly | QIODevice::Append);
        QTextStream text_stream014(&file);
        QString logmessage0;
        logmessage0 = "节点一同步完成";
        text_stream014 << logmessage0 << "\r\n";
        file.flush();
        file.close();


        emit updatesignal();
    } catch(std::exception& e)
    {
        emit exceptionsignal(e.what());
    }
}
void worker::DeDabaSecond()
{
    try {
        QVector<QString> TradeList;
        QVector<QString> HisList;
        QVector<QString> FieldList;
        QVector<QString> HisFieldList;

        QString year = this->dt.mid(0,4);
        QString month = this->dt.mid(4,2);

        QString Currentpath = QDir::currentPath();
        QString TodayDirpath = Currentpath;
        TodayDirpath.append("/").append(this->dt);
        QString Server2Path = TodayDirpath;
        Server2Path.append("/server2");
        QDir dir(TodayDirpath);
        QDir dirserver2(Server2Path);

        QString Secondlog = Currentpath;
        Secondlog.append("/log/");
        QDir dirlog(Secondlog);
        if(!dirlog.exists())
        {
            dirlog.mkdir(Secondlog);//创建log目录
        }
        //
        Secondlog.append(this->dt);
        //
        QDir dirlogday(Secondlog);
        if(!dirlogday.exists())
        {
            dirlogday.mkdir(Secondlog);//创建log/日期目录
        }
        //
        Secondlog.append("/server2.log");
        QFile file(Secondlog);
        QString logmessage;

        if(!dir.exists())
        {
            dir.mkdir(TodayDirpath);//只创建一级子目录，即必须保证上级目录存在
        }
        if(!dirserver2.exists())
        {
            dirserver2.mkdir(Server2Path);
        }
        else if(!dirserver2.isEmpty())
        {
            //先删除目录，再创建目录
            dirserver2.removeRecursively();
            dirserver2.mkdir(Server2Path);
        }
        //读取配置信息
        QString iniPath = Currentpath.append("/config.ini");
        getconfig(2,iniPath, "/secondhisserver/user", "/secondhisserver/password", "/secondhisserver/hostname", "/secondhisserver/port", "/secondhisserver/changepassword");
        getconfig(0,iniPath, "/zkhisserver/user", "/zkhisserver/password", "/zkhisserver/hostname", "/zkhisserver/port", "/zkhisserver/changepassword");

        readtratable(iniPath,trade_count, TradeList, FieldList,"/trade/tab","/trade/field",year,month);
        readtratable(iniPath,his_count, HisList, HisFieldList,"/history/tab","/history/field",year,month);

        int progress_count = 0; //已经复制的表的数量
        float progress = 1.00; //进度条数据
        QDateTime * starttime2 = new QDateTime();
        QString begintime;

        //进行数据核对 db1是总控历史run库   db2是总控历史his库  db3是节点一历史run库   db4是节点一历史his库
        QSqlDatabase db1;
        QSqlDatabase db2;
        QSqlDatabase db3;
        QSqlDatabase db4;
        myconnect(db1, "zkrunsecond", hisserver_host[0], hisserver_user[0], hisserver_password[0], "run");
        myconnect(db2, "zkhissecond", hisserver_host[0], hisserver_user[0], hisserver_password[0], "his");
        myconnect(db3, "secondrun", hisserver_host[2], hisserver_user[2], hisserver_password[2], "run");
        myconnect(db4, "secondhis", hisserver_host[2], hisserver_user[2], hisserver_password[2], "his");
        QSqlQuery zkqueryrun(db1);
        QSqlQuery zkqueryhis(db2);
        QSqlQuery secondqueryrun(db3);
        QSqlQuery secondqueryhis(db4);

        for(int i=0; i<trade_count; i++)
        {
            progress_count++;
            progress = (progress_count/float((trade_count+his_count)*2))*100;
            QString tabname = TradeList[i];
            QString fieldname = FieldList[i];
            QString sql = "select * from run..";
            sql.append(tabname);
            sql.append(" where serverid = '2'");
            if(fieldname != "-1")
            {
                sql.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            QProcess process(this);
            QString cmdin = "bcp run..";
            cmdin.append(tabname);
            cmdin.append(" in ");
            cmdin.append(Server2Path);
            cmdin.append("\\");
            cmdin.append(tabname);
            cmdin.append(".bcp  -S");
            cmdin.append(hisserver_ip[0]);
            cmdin.append(",");
            cmdin.append(hisserver_port[0]);
            cmdin.append(" -U");
            cmdin.append(hisserver_user[0]);
            cmdin.append(" -P");
            cmdin.append(hisserver_password[0]);
            cmdin.append(" -n");
            QString cmd = "bcp \"";
            cmd.append(sql).append("\"").append(" queryout ").append(Server2Path).append("\\").append(tabname).append(".bcp  -S").append(hisserver_ip[2]).append(",").append(hisserver_port[2]).append(" -U").append(hisserver_user[2]).append(" -P").append(hisserver_password[2]).append(" -n");
            begintime = starttime2->currentDateTime().toString("yyyy.MM.dd hh:mm:ss");
            process.start(cmd);
            file.open(QIODevice::WriteOnly | QIODevice::Append);
            QTextStream text_stream1(&file);
            logmessage = "server2:";
            logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append("bcp out table ").append(tabname).append(":").append(sql);
            text_stream1 << logmessage << "\r\n";
            file.flush();
            file.close();
            emit showNum(2,progress,1,tabname,begintime);//触发信号
            process.waitForFinished(-1);


            QString sql_orderrec="delete  from run..";
            sql_orderrec.append(tabname);
            sql_orderrec.append(" where serverid = '2'");
            if(fieldname != "-1")
            {
                sql_orderrec.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            bool result = false;
            while(!result)
            {
                result = zkqueryrun.exec(sql_orderrec);
            }
            if(result)
            {
                db1.commit();
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream2(&file);
                logmessage = "server2:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append(sql_orderrec);
                text_stream2 << logmessage << "\r\n";
                file.flush();
                file.close();

                progress_count++;
                progress = (progress_count/float((trade_count+his_count)*2))*100;
                QProcess processin(this);

                begintime = starttime2->currentDateTime().toString("yyyy.MM.dd hh:mm:ss");
                processin.start(cmdin);
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream4(&file);
                logmessage = "server2:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append("bcp in ").append(tabname);
                text_stream4 << logmessage << "\r\n";
                file.flush();
                file.close();
                emit showNum(2,progress,2,tabname,begintime);//触发信号
                processin.waitForFinished(-1);
            }
            else
            {
                db1.rollback();
                QString me = "节点二删除表";
                me.append(tabname).append("失败").append(zkqueryrun.lastError().databaseText());
                emit exceptionsignal(me);
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream3(&file);
                logmessage = "server2:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":回滚:").append(sql_orderrec);
                text_stream3 <<logmessage <<"\r\n";
                file.flush();
                file.close();
            }


            //进行数据核对
            int secondruncount = 0;
            int zkruncount = 0;
            QString sql_secondruncheck="select count(*)  from run..";
            sql_secondruncheck.append(tabname);
            sql_secondruncheck.append(" WITH(NOLOCK) where serverid = '2'");
            if(fieldname != "-1")
            {
                sql_secondruncheck.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            secondqueryrun.exec(sql_secondruncheck);
            if(secondqueryrun.next())
            {
                secondruncount = secondqueryrun.value(0).toInt();
            }
            zkqueryrun.exec(sql_secondruncheck);
            if(zkqueryrun.next())
            {
                zkruncount = zkqueryrun.value(0).toInt();
            }
            file.open(QIODevice::WriteOnly | QIODevice::Append);
            QTextStream text_stream5(&file);
            logmessage = "server2:";
            logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("数据核对:").append(sql_secondruncheck);
            logmessage.append(" server2-").append(tabname).append("=").append(QString::number(secondruncount,10));
            logmessage.append(" zk-server2-").append(tabname).append("=").append(QString::number(zkruncount,10));
            text_stream5 << logmessage << "\r\n";
            file.flush();
            file.close();

            //第一次核对不平，重新执行bcp
            int checkcount = 0;
            while(checkcount < 5 && secondruncount != zkruncount)
            {
                checkcount++;
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream6(&file);
                logmessage = "server2:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("数据核对不平:").append(sql_secondruncheck);
                logmessage.append(" server2-").append(tabname).append("=").append(QString::number(secondruncount,10));
                logmessage.append(" zk-server2-").append(tabname).append("=").append(QString::number(zkruncount,10));
                text_stream6 << logmessage << "\r\n";
                logmessage = "开始下一遍bcp...";
                text_stream6 << logmessage << "\r\n";
                file.flush();
                file.close();

                //删除bcpout文件
                QString removepath = Server2Path;
                removepath.append("\\").append(tabname).append(".bcp");
                QFile::remove(removepath);

                QProcess processagain(this);
                processagain.start(cmd);
                processagain.waitForFinished(-1);
                bool result = false;
                while(!result)
                {
                    result = zkqueryrun.exec(sql_orderrec);
                }
                if(result)
                {
                    db1.commit();
                    QProcess processinagain(this);
                    processinagain.start(cmdin);
                    processinagain.waitForFinished(-1);
                }
                else
                {
                    db1.rollback();
                }

                secondqueryrun.exec(sql_secondruncheck);
                if(secondqueryrun.next())
                {
                    secondruncount = secondqueryrun.value(0).toInt();
                }
                zkqueryrun.exec(sql_secondruncheck);
                if(zkqueryrun.next())
                {
                    zkruncount = zkqueryrun.value(0).toInt();
                }
            }
            if(secondruncount != zkruncount)
            {
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream7(&file);
                logmessage = "server2:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("请人工处理不平数据:").append(sql_secondruncheck);
                logmessage.append(" server2-").append(tabname).append("=").append(QString::number(secondruncount,10));
                logmessage.append(" zk-server2-").append(tabname).append("=").append(QString::number(zkruncount,10));
                text_stream7 << logmessage << "\r\n";
                file.flush();
                file.close();
                emit showDiff("节点二",tabname,secondruncount,zkruncount,secondruncount-zkruncount);
            }

        }

        for(int k=0; k<his_count; k++)
        {
            progress_count++;
            progress = (progress_count/float((trade_count+his_count)*2))*100;
            QString tabname = HisList[k];
            QString fieldname = HisFieldList[k];
            QString sql = "select * from his..";
            sql.append(tabname);
            sql.append(" where serverid = '2'");
            if(fieldname != "-1")
            {
                sql.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            QProcess processhis(this);
            QString cmd = "bcp \"";
            cmd.append(sql);
            cmd.append("\"");
            cmd.append(" queryout ");
            cmd.append(Server2Path);
            cmd.append("\\");
            cmd.append(tabname);
            cmd.append(".bcp  -S");
            cmd.append(hisserver_ip[2]);
            cmd.append(",");
            cmd.append(hisserver_port[2]);
            cmd.append(" -U");
            cmd.append(hisserver_user[2]);
            cmd.append(" -P");
            cmd.append(hisserver_password[2]);
            cmd.append(" -n");
            QString cmdin = "bcp his..";
            cmdin.append(tabname);
            cmdin.append(" in ");
            cmdin.append(Server2Path);
            cmdin.append("\\");
            cmdin.append(tabname);
            cmdin.append(".bcp  -S");
            cmdin.append(hisserver_ip[0]);
            cmdin.append(",");
            cmdin.append(hisserver_port[0]);
            cmdin.append(" -U");
            cmdin.append(hisserver_user[0]);
            cmdin.append(" -P");
            cmdin.append(hisserver_password[0]);
            cmdin.append(" -n");
            begintime = starttime2->currentDateTime().toString("yyyy.MM.dd hh:mm:ss");
            processhis.start(cmd);
            file.open(QIODevice::WriteOnly | QIODevice::Append);
            QTextStream text_stream8(&file);
            logmessage = "server2:";
            logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append("bcp out table ").append(tabname).append(":").append(sql);
            text_stream8 << logmessage << "\r\n";
            file.flush();
            file.close();
            emit showNum(2,progress,1,tabname,begintime);//触发信号
            processhis.waitForFinished(-1);

            QString sql_orderrec="delete  from his..";
            sql_orderrec.append(tabname);
            sql_orderrec.append(" where serverid = '2'");
            if(fieldname != "-1")
            {
                sql_orderrec.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            bool result = false;
            while(!result)
            {
                result = zkqueryhis.exec(sql_orderrec);
            }
            if(result)
            {
                db2.commit();
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream9(&file);
                logmessage = "server2:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append(sql_orderrec);
                text_stream9 << logmessage << "\r\n";
                file.flush();
                file.close();

                progress_count++;
                progress = (progress_count/float((trade_count+his_count)*2))*100;
                QProcess processin(this);

                begintime = starttime2->currentDateTime().toString("yyyy.MM.dd hh:mm:ss");
                processin.start(cmdin);
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream11(&file);
                logmessage = "server2:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append("bcp in ").append(tabname);
                text_stream11 << logmessage << "\r\n";
                file.flush();
                file.close();
                emit showNum(2,progress,2,tabname,begintime);//触发信号
                processin.waitForFinished(-1);
            }
            else
            {
                db2.rollback();
                QString me = "节点二删除表";
                me.append(tabname).append("失败").append(zkqueryhis.lastError().databaseText());
                emit exceptionsignal(me);
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream10(&file);
                logmessage = "server2:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":回滚:").append(sql_orderrec);
                text_stream10 << logmessage << "\r\n";
                file.flush();
                file.close();
            }


            //进行数据核对
            int zkhiscount = 0;
            int secondhiscount = 0;
            QString sql_secondhischeck="select count(*)  from his..";
            sql_secondhischeck.append(tabname);
            sql_secondhischeck.append(" WITH(NOLOCK) where serverid = '2'");
            if(fieldname != "-1")
            {
                sql_secondhischeck.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            secondqueryhis.exec(sql_secondhischeck);
            if(secondqueryhis.next())
            {
                secondhiscount = secondqueryhis.value(0).toInt();
            }

            zkqueryhis.exec(sql_secondhischeck);
            if(zkqueryhis.next())
            {
                zkhiscount = zkqueryhis.value(0).toInt();
            }

            file.open(QIODevice::WriteOnly | QIODevice::Append);
            QTextStream text_stream12(&file);
            logmessage = "server2:";
            logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("数据核对:").append(sql_secondhischeck);
            logmessage.append(" server2-").append(tabname).append("=").append(QString::number(secondhiscount,10));
            logmessage.append(" zk-server2-").append(tabname).append("=").append(QString::number(zkhiscount,10));
            text_stream12 << logmessage << "\r\n";
            file.flush();
            file.close();

            //第一次核对不平，重新执行bcp
            int checkcount = 0;
            while(checkcount < 5 && secondhiscount != zkhiscount)
            {
                checkcount++;
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream13(&file);
                logmessage = "server2:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("数据核对不平:").append(sql_secondhischeck);
                logmessage.append(" server2-").append(tabname).append("=").append(QString::number(secondhiscount,10));
                logmessage.append(" zk-server2-").append(tabname).append("=").append(QString::number(zkhiscount,10));
                text_stream13 << logmessage << "\r\n";
                logmessage = "开始下一遍bcp...";
                text_stream13 << logmessage << "\r\n";
                file.flush();
                file.close();

                //删除bcpout文件
                QString removepath = Server2Path;
                removepath.append("\\").append(tabname).append(".bcp");
                QFile::remove(removepath);
                QProcess processagain(this);
                processagain.start(cmd);
                processagain.waitForFinished(-1);
                bool result = false;
                while(!result)
                {
                    result = zkqueryhis.exec(sql_orderrec);
                }
                if(result)
                {
                    db1.commit();
                    QProcess processinagain(this);
                    processinagain.start(cmdin);
                    processinagain.waitForFinished(-1);
                }
                else
                {
                    db1.rollback();
                }

                secondqueryhis.exec(sql_secondhischeck);
                if(secondqueryhis.next())
                {
                    secondhiscount = secondqueryhis.value(0).toInt();
                }
                zkqueryhis.exec(sql_secondhischeck);
                if(zkqueryhis.next())
                {
                    zkhiscount = zkqueryhis.value(0).toInt();
                }
            }
            if(secondhiscount != zkhiscount)
            {
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream14(&file);
                logmessage = "server2:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("请人工处理不平数据:").append(sql_secondhischeck);
                logmessage.append(" server2-").append(tabname).append("=").append(QString::number(secondhiscount,10));
                logmessage.append(" zk-server2-").append(tabname).append("=").append(QString::number(zkhiscount,10));
                text_stream14 << logmessage << "\r\n";
                file.flush();
                file.close();
                emit showDiff("节点二",tabname,secondhiscount,zkhiscount,secondhiscount-zkhiscount);
            }
        }
        db1.close();
        db2.close();
        db3.close();
        db4.close();

        file.open(QIODevice::WriteOnly | QIODevice::Append);
        QTextStream text_stream114(&file);
        QString logmessage1;
        logmessage1 = "节点二同步完成";
        text_stream114 << logmessage1 << "\r\n";
        file.flush();
        file.close();

        emit updatesignal();
    } catch(std::exception& e)
    {
        emit exceptionsignal(e.what());
    }
}
void worker::DeDabaThird()
{
    try {
        QVector<QString> TradeList;
        QVector<QString> HisList;
        QVector<QString> FieldList;
        QVector<QString> HisFieldList;

        QString year = this->dt.mid(0,4);
        QString month = this->dt.mid(4,2);

        QString Currentpath = QDir::currentPath();
        QString TodayDirpath = Currentpath;
        TodayDirpath.append("/").append(this->dt);
        QString Server3Path = TodayDirpath;
        Server3Path.append("/server3");
        QDir dir(TodayDirpath);
        QDir dirserver3(Server3Path);

        QString Thirdlog = Currentpath;
        Thirdlog.append("/log/");
        QDir dirlog(Thirdlog);
        if(!dirlog.exists())
        {
            dirlog.mkdir(Thirdlog);//创建log目录
        }
        //
        Thirdlog.append(this->dt);
        //
        QDir dirlogday(Thirdlog);
        if(!dirlogday.exists())
        {
            dirlogday.mkdir(Thirdlog);//创建log/日期目录
        }
        //
        Thirdlog.append("/server3.log");
        QFile file(Thirdlog);
        QString logmessage;

        if(!dir.exists())
        {
            dir.mkdir(TodayDirpath);//只创建一级子目录，即必须保证上级目录存在
        }
        if(!dirserver3.exists())
        {
            dirserver3.mkdir(Server3Path);
        }
        else if(!dirserver3.isEmpty())
        {
            //先删除目录，再创建目录
            dirserver3.removeRecursively();
            dirserver3.mkdir(Server3Path);
        }
        //读取配置信息
        QString iniPath = Currentpath.append("/config.ini");
        getconfig(3,iniPath, "/thirdhisserver/user", "/thirdhisserver/password", "/thirdhisserver/hostname", "/thirdhisserver/port", "/thirdhisserver/changepassword");
        getconfig(0,iniPath, "/zkhisserver/user", "/zkhisserver/password", "/zkhisserver/hostname", "/zkhisserver/port", "/zkhisserver/changepassword");

        readtratable(iniPath,trade_count, TradeList, FieldList,"/trade/tab","/trade/field",year,month);
        readtratable(iniPath,his_count, HisList, HisFieldList,"/history/tab","/history/field",year,month);

        int progress_count = 0; //已经复制的表的数量
        float progress = 1.00; //进度条数据
        QDateTime * starttime3 = new QDateTime();
        QString begintime;

        //进行数据核对 db1是总控历史run库   db2是总控历史his库  db3是节点一历史run库   db4是节点一历史his库
        QSqlDatabase db1;
        QSqlDatabase db2;
        QSqlDatabase db3;
        QSqlDatabase db4;
        myconnect(db1, "zkrunthird", hisserver_host[0], hisserver_user[0], hisserver_password[0], "run");
        myconnect(db2, "zkhisthird", hisserver_host[0], hisserver_user[0], hisserver_password[0], "his");
        myconnect(db3, "thirdrun", hisserver_host[3], hisserver_user[3], hisserver_password[3], "run");
        myconnect(db4, "thirdhis", hisserver_host[3], hisserver_user[3], hisserver_password[3], "his");
        QSqlQuery zkqueryrun(db1);
        QSqlQuery zkqueryhis(db2);
        QSqlQuery thirdqueryrun(db3);
        QSqlQuery thirdqueryhis(db4);

        for(int i=0; i<trade_count; i++)
        {
            progress_count++;
            progress = (progress_count/float((trade_count+his_count)*2))*100;
            QString tabname = TradeList[i];
            QString fieldname = FieldList[i];
            QString sql = "select * from run..";
            sql.append(tabname);
            sql.append(" where serverid = '3'");
            if(fieldname != "-1")
            {
                sql.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            QProcess process(this);
            QString cmdin = "bcp run..";
            cmdin.append(tabname);
            cmdin.append(" in ");
            cmdin.append(Server3Path);
            cmdin.append("\\");
            cmdin.append(tabname);
            cmdin.append(".bcp  -S");
            cmdin.append(hisserver_ip[0]);
            cmdin.append(",");
            cmdin.append(hisserver_port[0]);
            cmdin.append(" -U");
            cmdin.append(hisserver_user[0]);
            cmdin.append(" -P");
            cmdin.append(hisserver_password[0]);
            cmdin.append(" -n");
            QString cmd = "bcp \"";
            cmd.append(sql).append("\"").append(" queryout ").append(Server3Path).append("\\").append(tabname).append(".bcp  -S").append(hisserver_ip[3]).append(",").append(hisserver_port[3]).append(" -U").append(hisserver_user[3]).append(" -P").append(hisserver_password[3]).append(" -n");
            begintime = starttime3->currentDateTime().toString("yyyy.MM.dd hh:mm:ss");
            process.start(cmd);
            file.open(QIODevice::WriteOnly | QIODevice::Append);
            QTextStream text_stream1(&file);
            logmessage = "server3:";
            logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append("bcp out table ").append(tabname).append(":").append(sql);
            text_stream1 << logmessage << "\r\n";
            file.flush();
            file.close();
            emit showNum(3,progress,1,tabname,begintime);//触发信号
            process.waitForFinished(-1);


            QString sql_orderrec="delete  from run..";
            sql_orderrec.append(tabname);
            sql_orderrec.append(" where serverid = '3'");
            if(fieldname != "-1")
            {
                sql_orderrec.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            bool result = false;
            while(!result)
            {
                result = zkqueryrun.exec(sql_orderrec);
            }
            if(result)
            {
                db1.commit();
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream2(&file);
                logmessage = "server3:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append(sql_orderrec);
                text_stream2 << logmessage << "\r\n";
                file.flush();
                file.close();

                progress_count++;
                progress = (progress_count/float((trade_count+his_count)*2))*100;
                QProcess processin(this);

                begintime = starttime3->currentDateTime().toString("yyyy.MM.dd hh:mm:ss");
                processin.start(cmdin);
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream4(&file);
                logmessage = "server3:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append("bcp in ").append(tabname);
                text_stream4 << logmessage << "\r\n";
                file.flush();
                file.close();
                emit showNum(3,progress,2,tabname,begintime);//触发信号
                processin.waitForFinished(-1);
            }
            else
            {
                db1.rollback();
                QString me = "节点三删除表";
                me.append(tabname).append("失败").append(zkqueryrun.lastError().databaseText());
                emit exceptionsignal(me);
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream3(&file);
                logmessage = "server3:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":回滚:").append(sql_orderrec);
                text_stream3 << logmessage << "\r\n";
                file.flush();
                file.close();
            }


            //进行数据核对
            int thirdruncount = 0;
            int zkruncount = 0;
            QString sql_thirdruncheck="select count(*)  from run..";
            sql_thirdruncheck.append(tabname);
            sql_thirdruncheck.append(" WITH(NOLOCK)  where serverid = '3'");
            if(fieldname != "-1")
            {
                sql_thirdruncheck.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            thirdqueryrun.exec(sql_thirdruncheck);
            if(thirdqueryrun.next())
            {
                thirdruncount = thirdqueryrun.value(0).toInt();
            }
            zkqueryrun.exec(sql_thirdruncheck);
            if(zkqueryrun.next())
            {
                zkruncount = zkqueryrun.value(0).toInt();
            }

            file.open(QIODevice::WriteOnly | QIODevice::Append);
            QTextStream text_stream5(&file);
            logmessage = "server3:";
            logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("数据核对:").append(sql_thirdruncheck);
            logmessage.append(" server3-").append(tabname).append("=").append(QString::number(thirdruncount,10));
            logmessage.append(" zk-server3-").append(tabname).append("=").append(QString::number(zkruncount,10));
            text_stream5 << logmessage << "\r\n";
            file.flush();
            file.close();

            //第一次核对不平，重新执行bcp
            int checkcount = 0;
            while(checkcount < 5 && thirdruncount != zkruncount)
            {
                checkcount++;
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream6(&file);
                logmessage = "server3:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("数据核对不平:").append(sql_thirdruncheck);
                logmessage.append(" server3-").append(tabname).append("=").append(QString::number(thirdruncount,10));
                logmessage.append(" zk-server3-").append(tabname).append("=").append(QString::number(zkruncount,10));
                text_stream6 << logmessage << "\r\n";
                logmessage = "开始下一遍bcp...";
                text_stream6 << logmessage << "\r\n";
                file.flush();
                file.close();

                //删除bcpout文件
                QString removepath = Server3Path;
                removepath.append("\\").append(tabname).append(".bcp");
                QFile::remove(removepath);

                QProcess processagain(this);
                processagain.start(cmd);
                processagain.waitForFinished(-1);
                bool result = false;
                while(!result)
                {
                    result = zkqueryrun.exec(sql_orderrec);
                }
                if(result)
                {
                    db1.commit();
                    QProcess processinagain(this);
                    processinagain.start(cmdin);
                    processinagain.waitForFinished(-1);
                }
                else
                {
                    db1.rollback();
                }

                thirdqueryrun.exec(sql_thirdruncheck);
                if(thirdqueryrun.next())
                {
                    thirdruncount = thirdqueryrun.value(0).toInt();
                }
                zkqueryrun.exec(sql_thirdruncheck);
                if(zkqueryrun.next())
                {
                    zkruncount = zkqueryrun.value(0).toInt();
                }
            }
            if(thirdruncount != zkruncount)
            {
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream7(&file);
                logmessage = "server3:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("请人工处理不平数据:").append(sql_thirdruncheck);
                logmessage.append(" server3-").append(tabname).append("=").append(QString::number(thirdruncount,10));
                logmessage.append(" zk-server3-").append(tabname).append("=").append(QString::number(zkruncount,10));
                text_stream7 << logmessage << "\r\n";
                file.flush();
                file.close();
                emit showDiff("节点三",tabname,thirdruncount,zkruncount,thirdruncount-zkruncount);
            }

        }

        for(int k=0; k<his_count; k++)
        {
            progress_count++;
            progress = (progress_count/float((trade_count+his_count)*2))*100;
            QString tabname = HisList[k];
            QString fieldname = HisFieldList[k];
            QString sql = "select * from his..";
            sql.append(tabname);
            sql.append(" where serverid = '3'");
            if(fieldname != "-1")
            {
                sql.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            QProcess processhis(this);
            QString cmd = "bcp \"";
            cmd.append(sql);
            cmd.append("\"");
            cmd.append(" queryout ");
            cmd.append(Server3Path);
            cmd.append("\\");
            cmd.append(tabname);
            cmd.append(".bcp  -S");
            cmd.append(hisserver_ip[3]);
            cmd.append(",");
            cmd.append(hisserver_port[3]);
            cmd.append(" -U");
            cmd.append(hisserver_user[3]);
            cmd.append(" -P");
            cmd.append(hisserver_password[3]);
            cmd.append(" -n");
            QString cmdin = "bcp his..";
            cmdin.append(tabname);
            cmdin.append(" in ");
            cmdin.append(Server3Path);
            cmdin.append("\\");
            cmdin.append(tabname);
            cmdin.append(".bcp  -S");
            cmdin.append(hisserver_ip[0]);
            cmdin.append(",");
            cmdin.append(hisserver_port[0]);
            cmdin.append(" -U");
            cmdin.append(hisserver_user[0]);
            cmdin.append(" -P");
            cmdin.append(hisserver_password[0]);
            cmdin.append(" -n");
            begintime = starttime3->currentDateTime().toString("yyyy.MM.dd hh:mm:ss");
            processhis.start(cmd);
            file.open(QIODevice::WriteOnly | QIODevice::Append);
            QTextStream text_stream8(&file);
            logmessage = "server3:";
            logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append("bcp out table ").append(tabname).append(":").append(sql);
            text_stream8 << logmessage << "\r\n";
            file.flush();
            file.close();
            emit showNum(3,progress,1,tabname,begintime);//触发信号
            processhis.waitForFinished(-1);

            QString sql_orderrec="delete  from his..";
            sql_orderrec.append(tabname);
            sql_orderrec.append(" where serverid = '3'");
            if(fieldname != "-1")
            {
                sql_orderrec.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            bool result = false;
            while(!result)
            {
                result = zkqueryhis.exec(sql_orderrec);
            }
            if(result)
            {
                db2.commit();
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream9(&file);
                logmessage = "server3:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append(sql_orderrec);
                text_stream9 << logmessage << "\r\n";
                file.flush();
                file.close();

                progress_count++;
                progress = (progress_count/float((trade_count+his_count)*2))*100;
                QProcess processin(this);

                begintime = starttime3->currentDateTime().toString("yyyy.MM.dd hh:mm:ss");
                processin.start(cmdin);
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream11(&file);
                logmessage = "server3:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append("bcp in ").append(tabname);
                text_stream11 << logmessage << "\r\n";
                file.flush();
                file.close();
                emit showNum(3,progress,2,tabname,begintime);//触发信号
                processin.waitForFinished(-1);
            }
            else
            {
                db2.rollback();
                QString me = "节点三删除表";
                me.append(tabname).append("失败").append(zkqueryhis.lastError().databaseText());
                emit exceptionsignal(me);
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream10(&file);
                logmessage = "server3:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":回滚:").append(sql_orderrec);
                text_stream10 << logmessage << "\r\n";
                file.flush();
                file.close();
            }


            //进行数据核对
            int zkhiscount = 0;
            int thirdhiscount = 0;
            QString sql_thirdhischeck="select count(*)  from his..";
            sql_thirdhischeck.append(tabname);
            sql_thirdhischeck.append(" WITH(NOLOCK) where serverid = '3'");
            if(fieldname != "-1")
            {
                sql_thirdhischeck.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            thirdqueryhis.exec(sql_thirdhischeck);
            if(thirdqueryhis.next())
            {
                thirdhiscount = thirdqueryhis.value(0).toInt();
            }

            zkqueryhis.exec(sql_thirdhischeck);
            if(zkqueryhis.next())
            {
                zkhiscount = zkqueryhis.value(0).toInt();
            }
            file.open(QIODevice::WriteOnly | QIODevice::Append);
            QTextStream text_stream12(&file);
            logmessage = "server3:";
            logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("数据核对:").append(sql_thirdhischeck);
            logmessage.append(" server3-").append(tabname).append("=").append(QString::number(thirdhiscount,10));
            logmessage.append(" zk-server3-").append(tabname).append("=").append(QString::number(zkhiscount,10));
            text_stream12 << logmessage << "\r\n";
            file.flush();
            file.close();

            //第一次核对不平，重新执行bcp
            int checkcount = 0;
            while(checkcount < 5 && thirdhiscount != zkhiscount)
            {
                checkcount++;
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream13(&file);
                logmessage = "server3:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("数据核对不平:").append(sql_thirdhischeck);
                logmessage.append(" server3-").append(tabname).append("=").append(QString::number(thirdhiscount,10));
                logmessage.append(" zk-server3-").append(tabname).append("=").append(QString::number(zkhiscount,10));
                text_stream13 << logmessage << "\r\n";
                logmessage = "开始下一遍bcp...";
                text_stream13 << logmessage << "\r\n";
                file.flush();
                file.close();

                //删除bcpout文件
                QString removepath = Server3Path;
                removepath.append("\\").append(tabname).append(".bcp");
                QFile::remove(removepath);
                QProcess processagain(this);
                processagain.start(cmd);
                processagain.waitForFinished(-1);
                bool result = false;
                while(!result)
                {
                    result = zkqueryhis.exec(sql_orderrec);
                }
                if(result)
                {
                    db1.commit();
                    QProcess processinagain(this);
                    processinagain.start(cmdin);
                    processinagain.waitForFinished(-1);
                }
                else
                {
                    db1.rollback();
                }

                thirdqueryhis.exec(sql_thirdhischeck);
                if(thirdqueryhis.next())
                {
                    thirdhiscount = thirdqueryhis.value(0).toInt();
                }
                zkqueryhis.exec(sql_thirdhischeck);
                if(zkqueryhis.next())
                {
                    zkhiscount = zkqueryhis.value(0).toInt();
                }
            }
            if(thirdhiscount != zkhiscount)
            {
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream14(&file);
                logmessage = "server3:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("请人工处理不平数据:").append(sql_thirdhischeck);
                logmessage.append(" server3-").append(tabname).append("=").append(QString::number(thirdhiscount,10));
                logmessage.append(" zk-server3-").append(tabname).append("=").append(QString::number(zkhiscount,10));
                text_stream14 << logmessage << "\r\n";
                file.flush();
                file.close();
                emit showDiff("节点三",tabname,thirdhiscount,zkhiscount,thirdhiscount-zkhiscount);
            }
        }
        db1.close();
        db2.close();
        db3.close();
        db4.close();

        file.open(QIODevice::WriteOnly | QIODevice::Append);
        QTextStream text_stream214(&file);
        QString logmessage2;
        logmessage2 = "节点三同步完成";
        text_stream214 << logmessage2 << "\r\n";
        file.flush();
        file.close();

        emit updatesignal();
    } catch(std::exception& e)
    {
        emit exceptionsignal(e.what());
    }
}
void worker::DeDabaFourth()
{
    try {
        QVector<QString> TradeList;
        QVector<QString> HisList;
        QVector<QString> FieldList;
        QVector<QString> HisFieldList;

        QString year = this->dt.mid(0,4);
        QString month = this->dt.mid(4,2);

        QString Currentpath = QDir::currentPath();
        QString TodayDirpath = Currentpath;
        TodayDirpath.append("/").append(this->dt);
        QString Server4Path = TodayDirpath;
        Server4Path.append("/server4");
        QDir dir(TodayDirpath);
        QDir dirserver4(Server4Path);

        QString Fourthlog = Currentpath;
        Fourthlog.append("/log/");
        QDir dirlog(Fourthlog);
        if(!dirlog.exists())
        {
            dirlog.mkdir(Fourthlog);//创建log目录
        }
        //
        Fourthlog.append(this->dt);
        //
        QDir dirlogday(Fourthlog);
        if(!dirlogday.exists())
        {
            dirlogday.mkdir(Fourthlog);//创建log/日期目录
        }
        Fourthlog.append("/server4.log");
        QFile file(Fourthlog);
        QString logmessage;

        if(!dir.exists())
        {
            dir.mkdir(TodayDirpath);//只创建一级子目录，即必须保证上级目录存在
        }
        if(!dirserver4.exists())
        {
            dirserver4.mkdir(Server4Path);
        }
        else if(!dirserver4.isEmpty())
        {
            //先删除目录，再创建目录
            dirserver4.removeRecursively();
            dirserver4.mkdir(Server4Path);
        }
        //读取配置信息
        QString iniPath = Currentpath.append("/config.ini");
        getconfig(4,iniPath, "/forthhisserver/user", "/forthhisserver/password", "/forthhisserver/hostname", "/forthhisserver/port", "/forthhisserver/changepassword");
        getconfig(0,iniPath, "/zkhisserver/user", "/zkhisserver/password", "/zkhisserver/hostname", "/zkhisserver/port", "/zkhisserver/changepassword");

        readtratable(iniPath,trade_count, TradeList, FieldList,"/trade/tab","/trade/field",year,month);
        readtratable(iniPath,his_count, HisList, HisFieldList,"/history/tab","/history/field",year,month);

        int progress_count = 0; //已经复制的表的数量
        float progress = 1.00; //进度条数据
        QDateTime * starttime4 = new QDateTime();
        QString begintime;

        //进行数据核对 db1是总控历史run库   db2是总控历史his库  db3是节点一历史run库   db4是节点一历史his库
        QSqlDatabase db1;
        QSqlDatabase db2;
        QSqlDatabase db3;
        QSqlDatabase db4;
        myconnect(db1, "zkrunfourth", hisserver_host[0], hisserver_user[0], hisserver_password[0], "run");
        myconnect(db2, "zkhisfourth", hisserver_host[0], hisserver_user[0], hisserver_password[0], "his");
        myconnect(db3, "fourthrun", hisserver_host[4], hisserver_user[4], hisserver_password[4], "run");
        myconnect(db4, "fourthhis", hisserver_host[4], hisserver_user[4], hisserver_password[4], "his");
        QSqlQuery zkqueryrun(db1);
        QSqlQuery zkqueryhis(db2);
        QSqlQuery fourthqueryrun(db3);
        QSqlQuery fourthqueryhis(db4);

        for(int i=0; i<trade_count; i++)
        {
            progress_count++;
            progress = (progress_count/float((trade_count+his_count)*2))*100;
            QString tabname = TradeList[i];
            QString fieldname = FieldList[i];
            QString sql = "select * from run..";
            sql.append(tabname);
            sql.append(" where serverid = '4'");
            if(fieldname != "-1")
            {
                sql.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            QProcess process(this);
            QString cmdin = "bcp run..";
            cmdin.append(tabname);
            cmdin.append(" in ");
            cmdin.append(Server4Path);
            cmdin.append("\\");
            cmdin.append(tabname);
            cmdin.append(".bcp -S");
            cmdin.append(hisserver_ip[0]);
            cmdin.append(",");
            cmdin.append(hisserver_port[0]);
            cmdin.append(" -U");
            cmdin.append(hisserver_user[0]);
            cmdin.append(" -P");
            cmdin.append(hisserver_password[0]);
            cmdin.append(" -n");
            QString cmd = "bcp \"";
            cmd.append(sql).append("\"").append(" queryout ").append(Server4Path).append("\\").append(tabname).append(".bcp -S").append(hisserver_ip[4]).append(",").append(hisserver_port[4]).append(" -U").append(hisserver_user[4]).append(" -P").append(hisserver_password[4]).append(" -n");
            begintime = starttime4->currentDateTime().toString("yyyy.MM.dd hh:mm:ss");
            process.start(cmd);
            file.open(QIODevice::WriteOnly | QIODevice::Append);
            QTextStream text_stream1(&file);
            logmessage = "server4:";
            logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append("bcp out table ").append(tabname).append(":").append(sql);
            text_stream1 << logmessage << "\r\n";
            file.flush();
            file.close();
            emit showNum(4,progress,1,tabname,begintime);//触发信号
            process.waitForFinished(-1);


            QString sql_orderrec="delete  from run..";
            sql_orderrec.append(tabname);
            sql_orderrec.append(" where serverid = '4'");
            if(fieldname != "-1")
            {
                sql_orderrec.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            bool result = false;
            while(!result)
            {
                result = zkqueryrun.exec(sql_orderrec);
            }
            if(result)
            {
                db1.commit();
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream2(&file);
                logmessage = "server4:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append(sql_orderrec);
                text_stream2 << logmessage << "\r\n";
                file.flush();
                file.close();

                progress_count++;
                progress = (progress_count/float((trade_count+his_count)*2))*100;
                QProcess processin(this);

                begintime = starttime4->currentDateTime().toString("yyyy.MM.dd hh:mm:ss");
                processin.start(cmdin);
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream4(&file);
                logmessage = "server4:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append("bcp in ").append(tabname);
                text_stream4 << logmessage << "\r\n";
                file.flush();
                file.close();
                emit showNum(4,progress,2,tabname,begintime);//触发信号
                processin.waitForFinished(-1);
            }
            else
            {
                db1.rollback();
                QString me = "节点四删除表";
                me.append(tabname).append("失败").append(zkqueryrun.lastError().databaseText());
                emit exceptionsignal(me);
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream3(&file);
                logmessage = "server4:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":回滚:").append(sql_orderrec);
                text_stream3 << logmessage << "\r\n";
            }


            //进行数据核对
            int fourthruncount = 0;
            int zkruncount = 0;
            QString sql_fourthruncheck="select count(*)  from run..";
            sql_fourthruncheck.append(tabname);
            sql_fourthruncheck.append(" WITH(NOLOCK) where serverid = '4'");
            if(fieldname != "-1")
            {
                sql_fourthruncheck.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            fourthqueryrun.exec(sql_fourthruncheck);
            if(fourthqueryrun.next())
            {
                fourthruncount = fourthqueryrun.value(0).toInt();
            }
            zkqueryrun.exec(sql_fourthruncheck);
            if(zkqueryrun.next())
            {
                zkruncount = zkqueryrun.value(0).toInt();
            }

            file.open(QIODevice::WriteOnly | QIODevice::Append);
            QTextStream text_stream5(&file);
            logmessage = "server4:";
            logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("数据核对:").append(sql_fourthruncheck);
            logmessage.append(" server4-").append(tabname).append("=").append(QString::number(fourthruncount,10));
            logmessage.append(" zk-server4-").append(tabname).append("=").append(QString::number(zkruncount,10));
            text_stream5 << logmessage << "\r\n";
            file.flush();
            file.close();

            //第一次核对不平，重新执行bcp
            int checkcount = 0;
            while(checkcount < 5 && fourthruncount != zkruncount)
            {
                checkcount++;
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream6(&file);
                logmessage = "server4:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("数据核对不平:").append(sql_fourthruncheck);
                logmessage.append(" server4-").append(tabname).append("=").append(QString::number(fourthruncount,10));
                logmessage.append(" zk-server4-").append(tabname).append("=").append(QString::number(zkruncount,10));
                text_stream6 << logmessage << "\r\n";
                logmessage = "开始下一遍bcp...";
                text_stream6 << logmessage << "\r\n";
                file.flush();
                file.close();

                QString removepath = Server4Path;
                removepath.append("\\").append(tabname).append(".bcp");
                QFile::remove(removepath);

                QProcess processagain(this);
                processagain.start(cmd);
                processagain.waitForFinished(-1);
                bool result = false;
                while(!result)
                {
                    result = zkqueryrun.exec(sql_orderrec);
                }
                if(result)
                {
                    db1.commit();
                    QProcess processinagain(this);
                    processinagain.start(cmdin);
                    processinagain.waitForFinished(-1);
                }
                else
                {
                    db1.rollback();
                }

                fourthqueryrun.exec(sql_fourthruncheck);
                if(fourthqueryrun.next())
                {
                    fourthruncount = fourthqueryrun.value(0).toInt();
                }
                zkqueryrun.exec(sql_fourthruncheck);
                if(zkqueryrun.next())
                {
                    zkruncount = zkqueryrun.value(0).toInt();
                }
            }
            if(fourthruncount != zkruncount)
            {
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream7(&file);
                logmessage = "server4:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("请人工处理不平数据:").append(sql_fourthruncheck);
                logmessage.append(" server4-").append(tabname).append("=").append(QString::number(fourthruncount,10));
                logmessage.append(" zk-server4-").append(tabname).append("=").append(QString::number(zkruncount,10));
                text_stream7 << logmessage << "\r\n";
                file.flush();
                file.close();
                emit showDiff("节点四",tabname,fourthruncount,zkruncount,fourthruncount-zkruncount);
            }

        }

        for(int k=0; k<his_count; k++)
        {
            progress_count++;
            progress = (progress_count/float((trade_count+his_count)*2))*100;
            QString tabname = HisList[k];
            QString fieldname = HisFieldList[k];
            QString sql = "select * from his..";
            sql.append(tabname);
            sql.append(" where serverid = '4'");
            if(fieldname != "-1")
            {
                sql.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            QProcess processhis(this);
            QString cmd = "bcp \"";
            cmd.append(sql);
            cmd.append("\"");
            cmd.append(" queryout ");
            cmd.append(Server4Path);
            cmd.append("\\");
            cmd.append(tabname);
            cmd.append(".bcp -S");
            cmd.append(hisserver_ip[4]);
            cmd.append(",");
            cmd.append(hisserver_port[4]);
            cmd.append(" -U");
            cmd.append(hisserver_user[4]);
            cmd.append(" -P");
            cmd.append(hisserver_password[4]);
            cmd.append(" -n");
            QString cmdin = "bcp his..";
            cmdin.append(tabname);
            cmdin.append(" in ");
            cmdin.append(Server4Path);
            cmdin.append("\\");
            cmdin.append(tabname);
            cmdin.append(".bcp -S");
            cmdin.append(hisserver_ip[0]);
            cmdin.append(",");
            cmdin.append(hisserver_port[0]);
            cmdin.append(" -U");
            cmdin.append(hisserver_user[0]);
            cmdin.append(" -P");
            cmdin.append(hisserver_password[0]);
            cmdin.append(" -n");
            begintime = starttime4->currentDateTime().toString("yyyy.MM.dd hh:mm:ss");
            processhis.start(cmd);
            file.open(QIODevice::WriteOnly | QIODevice::Append);
            QTextStream text_stream8(&file);
            logmessage = "server4:";
            logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append("bcp out table ").append(tabname).append(":").append(sql);
            text_stream8 << logmessage << "\r\n";
            file.flush();
            file.close();
            emit showNum(4,progress,1,tabname,begintime);//触发信号
            processhis.waitForFinished(-1);

            QString sql_orderrec="delete  from his..";
            sql_orderrec.append(tabname);
            sql_orderrec.append(" where serverid = '4'");
            if(fieldname != "-1")
            {
                sql_orderrec.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            bool result = false;
            while(!result)
            {
                result = zkqueryhis.exec(sql_orderrec);
            }
            if(result)
            {
                db2.commit();
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream9(&file);
                logmessage = "server4:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append(sql_orderrec);
                text_stream9 << logmessage << "\r\n";
                file.flush();
                file.close();

                progress_count++;
                progress = (progress_count/float((trade_count+his_count)*2))*100;
                QProcess processin(this);

                begintime = starttime4->currentDateTime().toString("yyyy.MM.dd hh:mm:ss");
                processin.start(cmdin);
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream11(&file);
                logmessage = "server4:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append("bcp in ").append(tabname);
                text_stream11 << logmessage << "\r\n";
                file.flush();
                file.close();
                emit showNum(4,progress,2,tabname,begintime);//触发信号
                processin.waitForFinished(-1);
            }
            else
            {
                db2.rollback();
                QString me = "节点四删除表";
                me.append(tabname).append("失败").append(zkqueryhis.lastError().databaseText());
                emit exceptionsignal(me);
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream10(&file);
                logmessage = "server4:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":回滚:").append(sql_orderrec);
                text_stream10 << logmessage <<"\r\n";
                file.flush();
                file.close();
            }


            //进行数据核对
            int zkhiscount = 0;
            int fourthhiscount = 0;
            QString sql_fourthhischeck="select count(*)  from his..";
            sql_fourthhischeck.append(tabname);
            sql_fourthhischeck.append(" WITH(NOLOCK) where serverid = '4'");
            if(fieldname != "-1")
            {
                sql_fourthhischeck.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            fourthqueryhis.exec(sql_fourthhischeck);
            if(fourthqueryhis.next())
            {
                fourthhiscount = fourthqueryhis.value(0).toInt();
            }

            zkqueryhis.exec(sql_fourthhischeck);
            if(zkqueryhis.next())
            {
                zkhiscount = zkqueryhis.value(0).toInt();
            }

            file.open(QIODevice::WriteOnly | QIODevice::Append);
            QTextStream text_stream12(&file);
            logmessage = "server4:";
            logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("数据核对:").append(sql_fourthhischeck);
            logmessage.append(" server4-").append(tabname).append("=").append(QString::number(fourthhiscount,10));
            logmessage.append(" zk-server4-").append(tabname).append("=").append(QString::number(zkhiscount,10));
            text_stream12 << logmessage << "\r\n";
            file.flush();
            file.close();

            //第一次核对不平，重新执行bcp
            int checkcount = 0;
            while(checkcount < 5 && fourthhiscount != zkhiscount)
            {
                checkcount++;
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream13(&file);
                logmessage = "server4:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("数据核对不平:").append(sql_fourthhischeck);
                logmessage.append(" server4-").append(tabname).append("=").append(QString::number(fourthhiscount,10));
                logmessage.append(" zk-server4-").append(tabname).append("=").append(QString::number(zkhiscount,10));
                text_stream13 << logmessage << "\r\n";
                logmessage = "开始下一遍bcp...";
                text_stream13 << logmessage << "\r\n";
                file.flush();
                file.close();
                QString removepath = Server4Path;
                removepath.append("\\").append(tabname).append(".bcp");
                QFile::remove(removepath);
                QProcess processagain(this);
                processagain.start(cmd);
                processagain.waitForFinished(-1);
                bool result = false;
                while(!result)
                {
                    result = zkqueryhis.exec(sql_orderrec);
                }
                if(result)
                {
                    db1.commit();
                    QProcess processinagain(this);
                    processinagain.start(cmdin);
                    processinagain.waitForFinished(-1);
                }
                else
                {
                    db1.rollback();
                }

                fourthqueryhis.exec(sql_fourthhischeck);
                if(fourthqueryhis.next())
                {
                    fourthhiscount = fourthqueryhis.value(0).toInt();
                }
                zkqueryhis.exec(sql_fourthhischeck);
                if(zkqueryhis.next())
                {
                    zkhiscount = zkqueryhis.value(0).toInt();
                }
            }
            if(fourthhiscount != zkhiscount)
            {
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream14(&file);
                logmessage = "server4:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("请人工处理不平数据:").append(sql_fourthhischeck);
                logmessage.append(" server4-").append(tabname).append("=").append(QString::number(fourthhiscount,10));
                logmessage.append(" zk-server4-").append(tabname).append("=").append(QString::number(zkhiscount,10));
                text_stream14 << logmessage << "\r\n";
                file.flush();
                file.close();
                emit showDiff("节点四",tabname,fourthhiscount,zkhiscount,fourthhiscount-zkhiscount);
            }
        }
        db1.close();
        db2.close();
        db3.close();
        db4.close();

        file.open(QIODevice::WriteOnly | QIODevice::Append);
        QTextStream text_stream314(&file);
        QString logmessage3;
        logmessage3 = "节点四同步完成";
        text_stream314 << logmessage3 << "\r\n";
        file.flush();
        file.close();

        emit updatesignal();
    } catch(std::exception& e)
    {
        emit exceptionsignal(e.what());
    }
}
void worker::DeDabaFifth()
{
    try {
        QVector<QString> TradeList;
        QVector<QString> HisList;
        QVector<QString> FieldList;
        QVector<QString> HisFieldList;

  //      QString Currentday = QDate::currentDate().toString("yyyyMMdd");
        QString year = this->dt.mid(0,4);
        QString month = this->dt.mid(4,2);

        QString Currentpath = QDir::currentPath();
        QString TodayDirpath = Currentpath;
        TodayDirpath.append("/").append(this->dt);
        QString Server5Path = TodayDirpath;
        Server5Path.append("/server5");
        QDir dir(TodayDirpath);
        QDir dirserver5(Server5Path);

        QString Fifthlog = Currentpath;
        Fifthlog.append("/log/");
        QDir dirlog(Fifthlog);
        if(!dirlog.exists())
        {
            dirlog.mkdir(Fifthlog);//创建log目录
        }
        //
        Fifthlog.append(this->dt);
        //
        QDir dirlogday(Fifthlog);
        if(!dirlogday.exists())
        {
            dirlogday.mkdir(Fifthlog);//创建log/日期目录
        }
        Fifthlog.append("/server5.log");
        QFile file(Fifthlog);
        QString logmessage;

        if(!dir.exists())
        {
            dir.mkdir(TodayDirpath);//只创建一级子目录，即必须保证上级目录存在
        }
        if(!dirserver5.exists())
        {
            dirserver5.mkdir(Server5Path);
        }
        else if(!dirserver5.isEmpty())
        {
            //先删除目录，再创建目录
            dirserver5.removeRecursively();
            dirserver5.mkdir(Server5Path);
        }
        //读取配置信息
        QString iniPath = Currentpath.append("/config.ini");
        getconfig(5,iniPath, "/fifthhisserver/user", "/fifthhisserver/password", "/fifthhisserver/hostname", "/fifthhisserver/port", "/fifthhisserver/changepassword");
        getconfig(0,iniPath, "/zkhisserver/user", "/zkhisserver/password", "/zkhisserver/hostname", "/zkhisserver/port", "/zkhisserver/changepassword");

        readtratable(iniPath,trade_count, TradeList, FieldList,"/trade/tab","/trade/field",year,month);
        readtratable(iniPath,his_count, HisList, HisFieldList,"/history/tab","/history/field",year,month);

        int progress_count = 0; //已经复制的表的数量
        float progress = 1.00; //进度条数据
        QDateTime * starttime5 = new QDateTime();
        QString begintime;

        //进行数据核对 db1是总控历史run库   db2是总控历史his库  db3是节点一历史run库   db4是节点一历史his库
        QSqlDatabase db1;
        QSqlDatabase db2;
        QSqlDatabase db3;
        QSqlDatabase db4;
        myconnect(db1, "zkrunfifth", hisserver_host[0], hisserver_user[0], hisserver_password[0], "run");
        myconnect(db2, "zkhisfifth", hisserver_host[0], hisserver_user[0], hisserver_password[0], "his");
        myconnect(db3, "fifthrun", hisserver_host[5], hisserver_user[5], hisserver_password[5], "run");
        myconnect(db4, "fifthhis", hisserver_host[5], hisserver_user[5], hisserver_password[5], "his");
        QSqlQuery zkqueryrun(db1);
        QSqlQuery zkqueryhis(db2);
        QSqlQuery fifthqueryrun(db3);
        QSqlQuery fifthqueryhis(db4);

        for(int i=0; i<trade_count; i++)
        {
            progress_count++;
            progress = (progress_count/float((trade_count+his_count)*2))*100;
            QString tabname = TradeList[i];
            QString fieldname = FieldList[i];
            QString sql = "select * from run..";
            sql.append(tabname);
            sql.append(" where serverid = '5'");
            if(fieldname != "-1")
            {
                sql.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            QProcess process(this);
            QString cmdin = "bcp run..";
            cmdin.append(tabname);
            cmdin.append(" in ");
            cmdin.append(Server5Path);
            cmdin.append("\\");
            cmdin.append(tabname);
            cmdin.append(".bcp  -S");
            cmdin.append(hisserver_ip[0]);
            cmdin.append(",");
            cmdin.append(hisserver_port[0]);
            cmdin.append(" -U");
            cmdin.append(hisserver_user[0]);
            cmdin.append(" -P");
            cmdin.append(hisserver_password[0]);
            cmdin.append(" -n");
            QString cmd = "bcp \"";
            cmd.append(sql).append("\"").append(" queryout ").append(Server5Path).append("\\").append(tabname).append(".bcp  -S").append(hisserver_ip[5]).append(",").append(hisserver_port[5]).append(" -U").append(hisserver_user[5]).append(" -P").append(hisserver_password[5]).append(" -n");
            begintime = starttime5->currentDateTime().toString("yyyy.MM.dd hh:mm:ss");
            process.start(cmd);
            file.open(QIODevice::WriteOnly | QIODevice::Append);
            QTextStream text_stream1(&file);
            logmessage = "server5:";
            logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append("bcp out table ").append(tabname).append(":").append(sql);
            text_stream1 << logmessage << "\r\n";
            file.flush();
            file.close();
            emit showNum(5,progress,1,tabname,begintime);//触发信号
            process.waitForFinished(-1);


            QString sql_orderrec="delete  from run..";
            sql_orderrec.append(tabname);
            sql_orderrec.append(" where serverid = '5'");
            if(fieldname != "-1")
            {
                sql_orderrec.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            bool result = false;
            while(!result)
            {
                result = zkqueryrun.exec(sql_orderrec);
            }
            if(result)
            {
                db1.commit();
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream2(&file);
                logmessage = "server5:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append(sql_orderrec);
                text_stream2 << logmessage << "\r\n";
                file.flush();
                file.close();

                progress_count++;
                progress = (progress_count/float((trade_count+his_count)*2))*100;
                QProcess processin(this);

                begintime = starttime5->currentDateTime().toString("yyyy.MM.dd hh:mm:ss");
                processin.start(cmdin);
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream4(&file);
                logmessage = "server5:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append("bcp in ").append(tabname);
                text_stream4 << logmessage << "\r\n";
                file.flush();
                file.close();
                emit showNum(5,progress,2,tabname,begintime);//触发信号
                processin.waitForFinished(-1);
            }
            else
            {
                db1.rollback();
                QString me = "节点五删除表";
                me.append(tabname).append("失败").append(zkqueryrun.lastError().databaseText());
                emit exceptionsignal(me);
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream3(&file);
                logmessage = "server5:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":回滚:").append(sql_orderrec);
                text_stream3 << logmessage << "\r\n";
                file.flush();
                file.close();
            }


            //进行数据核对
            int fifthruncount = 0;
            int zkruncount = 0;
            QString sql_fifthruncheck="select count(*)  from run..";
            sql_fifthruncheck.append(tabname);
            sql_fifthruncheck.append(" WITH(NOLOCK) where serverid = '5'");
            if(fieldname != "-1")
            {
                sql_fifthruncheck.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            fifthqueryrun.exec(sql_fifthruncheck);
            if(fifthqueryrun.next())
            {
                fifthruncount = fifthqueryrun.value(0).toInt();
            }
            zkqueryrun.exec(sql_fifthruncheck);
            if(zkqueryrun.next())
            {
                zkruncount = zkqueryrun.value(0).toInt();
            }

            file.open(QIODevice::WriteOnly | QIODevice::Append);
            QTextStream text_stream5(&file);
            logmessage = "server5:";
            logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("数据核对:").append(sql_fifthruncheck);
            logmessage.append(" server5-").append(tabname).append("=").append(QString::number(fifthruncount,10));
            logmessage.append(" zk-server5-").append(tabname).append("=").append(QString::number(zkruncount,10));
            text_stream5 << logmessage << "\r\n";
            file.flush();
            file.close();

            //第一次核对不平，重新执行bcp
            int checkcount = 0;
            while(checkcount < 5 && fifthruncount != zkruncount)
            {
                checkcount++;
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream6(&file);
                logmessage = "server5:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("数据核对不平:").append(sql_fifthruncheck);
                logmessage.append(" server5-").append(tabname).append("=").append(QString::number(fifthruncount,10));
                logmessage.append(" zk-server5-").append(tabname).append("=").append(QString::number(zkruncount,10));
                text_stream6 << logmessage << "\r\n";
                logmessage = "开始下一遍bcp...";
                text_stream6 << logmessage << "\r\n";
                file.flush();
                file.close();
                QString removepath = Server5Path;
                removepath.append("\\").append(tabname).append(".bcp");
                QFile::remove(removepath);

                QProcess processagain(this);
                processagain.start(cmd);
                processagain.waitForFinished(-1);
                bool result = false;
                while(!result)
                {
                    result = zkqueryrun.exec(sql_orderrec);
                }
                if(result)
                {
                    db1.commit();
                    QProcess processinagain(this);
                    processinagain.start(cmdin);
                    processinagain.waitForFinished(-1);
                }
                else
                {
                    db1.rollback();
                }

                fifthqueryrun.exec(sql_fifthruncheck);
                if(fifthqueryrun.next())
                {
                    fifthruncount = fifthqueryrun.value(0).toInt();
                }
                zkqueryrun.exec(sql_fifthruncheck);
                if(zkqueryrun.next())
                {
                    zkruncount = zkqueryrun.value(0).toInt();
                }
            }
            if(fifthruncount != zkruncount)
            {
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream7(&file);
                logmessage = "server5:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("请人工处理不平数据:").append(sql_fifthruncheck);
                logmessage.append(" server5-").append(tabname).append("=").append(QString::number(fifthruncount,10));
                logmessage.append(" zk-server5-").append(tabname).append("=").append(QString::number(zkruncount,10));
                text_stream7 << logmessage << "\r\n";
                file.flush();
                file.close();
                emit showDiff("节点五",tabname,fifthruncount,zkruncount,fifthruncount-zkruncount);
            }

        }

        for(int k=0; k<his_count; k++)
        {
            progress_count++;
            progress = (progress_count/float((trade_count+his_count)*2))*100;
            QString tabname = HisList[k];
            QString fieldname = HisFieldList[k];
            QString sql = "select * from his..";
            sql.append(tabname);
            sql.append(" where serverid = '5'");
            if(fieldname != "-1")
            {
                sql.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            QProcess processhis(this);
            QString cmd = "bcp \"";
            cmd.append(sql);
            cmd.append("\"");
            cmd.append(" queryout ");
            cmd.append(Server5Path);
            cmd.append("\\");
            cmd.append(tabname);
            cmd.append(".bcp  -S");
            cmd.append(hisserver_ip[5]);
            cmd.append(",");
            cmd.append(hisserver_port[5]);
            cmd.append(" -U");
            cmd.append(hisserver_user[5]);
            cmd.append(" -P");
            cmd.append(hisserver_password[5]);
            cmd.append(" -n");
            QString cmdin = "bcp his..";
            cmdin.append(tabname);
            cmdin.append(" in ");
            cmdin.append(Server5Path);
            cmdin.append("\\");
            cmdin.append(tabname);
            cmdin.append(".bcp  -S");
            cmdin.append(hisserver_ip[0]);
            cmdin.append(",");
            cmdin.append(hisserver_port[0]);
            cmdin.append(" -U");
            cmdin.append(hisserver_user[0]);
            cmdin.append(" -P");
            cmdin.append(hisserver_password[0]);
            cmdin.append(" -n");
            begintime = starttime5->currentDateTime().toString("yyyy.MM.dd hh:mm:ss");
            processhis.start(cmd);
            file.open(QIODevice::WriteOnly | QIODevice::Append);
            QTextStream text_stream8(&file);
            logmessage = "server5:";
            logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append("bcp out table ").append(tabname).append(":").append(sql);
            text_stream8 << logmessage << "\r\n";
            file.flush();
            file.close();
            emit showNum(5,progress,1,tabname,begintime);//触发信号
            processhis.waitForFinished(-1);

            QString sql_orderrec="delete  from his..";
            sql_orderrec.append(tabname);
            sql_orderrec.append(" where serverid = '5'");
            if(fieldname != "-1")
            {
                sql_orderrec.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            bool result = false;
            while(!result)
            {
                result = zkqueryhis.exec(sql_orderrec);
            }
            if(result)
            {
                db2.commit();
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream9(&file);
                logmessage = "server5:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append(sql_orderrec);
                text_stream9 << logmessage << "\r\n";
                file.flush();
                file.close();

                progress_count++;
                progress = (progress_count/float((trade_count+his_count)*2))*100;
                QProcess processin(this);

                begintime = starttime5->currentDateTime().toString("yyyy.MM.dd hh:mm:ss");
                processin.start(cmdin);
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream11(&file);
                logmessage = "server5:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append("bcp in ").append(tabname);
                text_stream11 << logmessage << "\r\n";
                file.flush();
                file.close();
                emit showNum(5,progress,2,tabname,begintime);//触发信号
                processin.waitForFinished(-1);
            }
            else
            {
                db2.rollback();
                QString me = "节点五删除表";
                me.append(tabname).append("失败").append(zkqueryhis.lastError().databaseText());
                emit exceptionsignal(me);
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream10(&file);
                logmessage = "server5:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":回滚:").append(sql_orderrec);
                text_stream10 << logmessage << "\r\n";
                file.flush();
                file.close();
            }


            //进行数据核对
            int zkhiscount = 0;
            int fifthhiscount = 0;
            QString sql_fifthhischeck="select count(*)  from his..";
            sql_fifthhischeck.append(tabname);
            sql_fifthhischeck.append(" WITH(NOLOCK) where serverid = '5'");
            if(fieldname != "-1")
            {
                sql_fifthhischeck.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            fifthqueryhis.exec(sql_fifthhischeck);
            if(fifthqueryhis.next())
            {
                fifthhiscount = fifthqueryhis.value(0).toInt();
            }

            zkqueryhis.exec(sql_fifthhischeck);
            if(zkqueryhis.next())
            {
                zkhiscount = zkqueryhis.value(0).toInt();
            }

            file.open(QIODevice::WriteOnly | QIODevice::Append);
            QTextStream text_stream12(&file);
            logmessage = "server5:";
            logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("数据核对:").append(sql_fifthhischeck);
            logmessage.append(" server5-").append(tabname).append("=").append(QString::number(fifthhiscount,10));
            logmessage.append(" zk-server5-").append(tabname).append("=").append(QString::number(zkhiscount,10));
            text_stream12 << logmessage << "\r\n";
            file.flush();
            file.close();

            //第一次核对不平，重新执行bcp
            int checkcount = 0;
            while(checkcount < 5 && fifthhiscount != zkhiscount)
            {
                checkcount++;
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream13(&file);
                logmessage = "server5:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("数据核对不平:").append(sql_fifthhischeck);
                logmessage.append(" server5-").append(tabname).append("=").append(QString::number(fifthhiscount,10));
                logmessage.append(" zk-server5-").append(tabname).append("=").append(QString::number(zkhiscount,10));
                text_stream13 << logmessage << "\r\n";
                logmessage = "开始下一遍bcp...";
                text_stream13 << logmessage << "\r\n";
                file.flush();
                file.close();
                QString removepath = Server5Path;
                removepath.append("\\").append(tabname).append(".bcp");
                QFile::remove(removepath);
                QProcess processagain(this);
                processagain.start(cmd);
                processagain.waitForFinished(-1);
                bool result = false;
                while(!result)
                {
                    result = zkqueryhis.exec(sql_orderrec);
                }
                if(result)
                {
                    db1.commit();
                    QProcess processinagain(this);
                    processinagain.start(cmdin);
                    processinagain.waitForFinished(-1);
                }
                else
                {
                    db1.rollback();
                }

                fifthqueryhis.exec(sql_fifthhischeck);
                if(fifthqueryhis.next())
                {
                    fifthhiscount = fifthqueryhis.value(0).toInt();
                }
                zkqueryhis.exec(sql_fifthhischeck);
                if(zkqueryhis.next())
                {
                    zkhiscount = zkqueryhis.value(0).toInt();
                }
            }
            if(fifthhiscount != zkhiscount)
            {
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream14(&file);
                logmessage = "server5:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("请人工处理不平数据:").append(sql_fifthhischeck);
                logmessage.append(" server5-").append(tabname).append("=").append(QString::number(fifthhiscount,10));
                logmessage.append(" zk-server5-").append(tabname).append("=").append(QString::number(zkhiscount,10));
                text_stream14 << logmessage << "\r\n";
                file.flush();
                file.close();
                emit showDiff("节点五",tabname,fifthhiscount,zkhiscount,fifthhiscount-zkhiscount);
            }
        }

        db1.close();
        db2.close();
        db3.close();
        db4.close();

        file.open(QIODevice::WriteOnly | QIODevice::Append);
        QTextStream text_stream414(&file);
        QString logmessage4;
        logmessage4 = "节点五同步完成";
        text_stream414 << logmessage4 << "\r\n";
        file.flush();
        file.close();

        emit updatesignal();
    } catch(std::exception& e)
    {
        emit exceptionsignal(e.what());
    }
}
void worker::DeDabaSixth()
{
    try {
        QVector<QString> TradeList;
        QVector<QString> HisList;
        QVector<QString> FieldList;
        QVector<QString> HisFieldList;

        QString year = this->dt.mid(0,4);
        QString month = this->dt.mid(4,2);

        QString Currentpath = QDir::currentPath();
        QString TodayDirpath = Currentpath;
        TodayDirpath.append("/").append(this->dt);
        QString Server6Path = TodayDirpath;
        Server6Path.append("/server6");
        QDir dir(TodayDirpath);
        QDir dirserver6(Server6Path);

        QString Sixthlog = Currentpath;
        Sixthlog.append("/log/");
        QDir dirlog(Sixthlog);
        if(!dirlog.exists())
        {
            dirlog.mkdir(Sixthlog);//创建log目录
        }
        //
        Sixthlog.append(this->dt);
        //
        QDir dirlogday(Sixthlog);
        if(!dirlogday.exists())
        {
            dirlogday.mkdir(Sixthlog);//创建log/日期目录
        }
        Sixthlog.append("/server6.log");
        QFile file(Sixthlog);
        QString logmessage;

        if(!dir.exists())
        {
            dir.mkdir(TodayDirpath);//只创建一级子目录，即必须保证上级目录存在
        }
        if(!dirserver6.exists())
        {
            dirserver6.mkdir(Server6Path);
        }
        else if(!dirserver6.isEmpty())
        {
            //先删除目录，再创建目录
            dirserver6.removeRecursively();
            dirserver6.mkdir(Server6Path);
        }
        //读取配置信息
        QString iniPath = Currentpath.append("/config.ini");
        getconfig(6,iniPath, "/sixthhisserver/user", "/sixthhisserver/password", "/sixthhisserver/hostname", "/sixthhisserver/port", "/sixthhisserver/changepassword");
        getconfig(0,iniPath, "/zkhisserver/user", "/zkhisserver/password", "/zkhisserver/hostname", "/zkhisserver/port", "/zkhisserver/changepassword");

        readtratable(iniPath,trade_count, TradeList, FieldList,"/trade/tab","/trade/field",year,month);
        readtratable(iniPath,his_count, HisList, HisFieldList,"/history/tab","/history/field",year,month);

        int progress_count = 0; //已经复制的表的数量
        float progress = 1.00; //进度条数据
        QDateTime * starttime6 = new QDateTime();
        QString begintime;

        //进行数据核对 db1是总控历史run库   db2是总控历史his库  db3是节点一历史run库   db4是节点一历史his库
        QSqlDatabase db1;
        QSqlDatabase db2;
        QSqlDatabase db3;
        QSqlDatabase db4;
        myconnect(db1, "zkrunsixth", hisserver_host[0], hisserver_user[0], hisserver_password[0], "run");
        myconnect(db2, "zkhissixth", hisserver_host[0], hisserver_user[0], hisserver_password[0], "his");
        myconnect(db3, "sixthrun", hisserver_host[6], hisserver_user[6], hisserver_password[6], "run");
        myconnect(db4, "sixthhis", hisserver_host[6], hisserver_user[6], hisserver_password[6], "his");
        QSqlQuery zkqueryrun(db1);
        QSqlQuery zkqueryhis(db2);
        QSqlQuery sixthqueryrun(db3);
        QSqlQuery sixthqueryhis(db4);

        for(int i=0; i<trade_count; i++)
        {
            progress_count++;
            progress = (progress_count/float((trade_count+his_count)*2))*100;
            QString tabname = TradeList[i];
            QString fieldname = FieldList[i];
            QString sql = "select * from run..";
            sql.append(tabname);
            sql.append(" where serverid = '6'");
            if(fieldname != "-1")
            {
                sql.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            QProcess process(this);
            QString cmdin = "bcp run..";
            cmdin.append(tabname);
            cmdin.append(" in ");
            cmdin.append(Server6Path);
            cmdin.append("\\");
            cmdin.append(tabname);
            cmdin.append(".bcp  -S");
            cmdin.append(hisserver_ip[0]);
            cmdin.append(",");
            cmdin.append(hisserver_port[0]);
            cmdin.append(" -U");
            cmdin.append(hisserver_user[0]);
            cmdin.append(" -P");
            cmdin.append(hisserver_password[0]);
            cmdin.append(" -n");
            QString cmd = "bcp \"";
            cmd.append(sql).append("\"").append(" queryout ").append(Server6Path).append("\\").append(tabname).append(".bcp  -S").append(hisserver_ip[6]).append(",").append(hisserver_port[6]).append(" -U").append(hisserver_user[6]).append(" -P").append(hisserver_password[6]).append(" -n");
            begintime = starttime6->currentDateTime().toString("yyyy.MM.dd hh:mm:ss");
            process.start(cmd);
            file.open(QIODevice::WriteOnly | QIODevice::Append);
            QTextStream text_stream1(&file);
            logmessage = "server6:";
            logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append("bcp out table ").append(tabname).append(":").append(sql);
            text_stream1 << logmessage << "\r\n";
            file.flush();
            file.close();
            emit showNum(6,progress,1,tabname,begintime);//触发信号
            process.waitForFinished(-1);


            QString sql_orderrec="delete  from run..";
            sql_orderrec.append(tabname);
            sql_orderrec.append(" where serverid = '6'");
            if(fieldname != "-1")
            {
                sql_orderrec.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            bool result = false;
            while(!result)
            {
                result = zkqueryrun.exec(sql_orderrec);
            }
            if(result)
            {
                db1.commit();
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream2(&file);
                logmessage = "server6:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append(sql_orderrec);
                text_stream2 << logmessage << "\r\n";
                file.flush();
                file.close();

                progress_count++;
                progress = (progress_count/float((trade_count+his_count)*2))*100;
                QProcess processin(this);

                begintime = starttime6->currentDateTime().toString("yyyy.MM.dd hh:mm:ss");
                processin.start(cmdin);
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream4(&file);
                logmessage = "server6:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append("bcp in ").append(tabname);
                text_stream4 << logmessage << "\r\n";
                file.flush();
                file.close();
                emit showNum(6,progress,2,tabname,begintime);//触发信号
                processin.waitForFinished(-1);
            }
            else
            {
                db1.rollback();
                QString me = "节点六删除表";
                me.append(tabname).append("失败").append(zkqueryrun.lastError().databaseText());
                emit exceptionsignal(me);
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream3(&file);
                logmessage = "server6:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":回滚:").append(sql_orderrec);
                text_stream3 << logmessage << "\r\n";
                file.flush();
                file.close();
            }


            //进行数据核对
            int sixthruncount = 0;
            int zkruncount = 0;
            QString sql_sixthruncheck="select count(*)  from run..";
            sql_sixthruncheck.append(tabname);
            sql_sixthruncheck.append(" WITH(NOLOCK) where serverid = '6'");
            if(fieldname != "-1")
            {
                sql_sixthruncheck.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            sixthqueryrun.exec(sql_sixthruncheck);
            if(sixthqueryrun.next())
            {
                sixthruncount = sixthqueryrun.value(0).toInt();
            }
            zkqueryrun.exec(sql_sixthruncheck);
            if(zkqueryrun.next())
            {
                zkruncount = zkqueryrun.value(0).toInt();
            }

            file.open(QIODevice::WriteOnly | QIODevice::Append);
            QTextStream text_stream5(&file);
            logmessage = "server6:";
            logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("数据核对:").append(sql_sixthruncheck);
            logmessage.append(" server6-").append(tabname).append("=").append(QString::number(sixthruncount,10));
            logmessage.append(" zk-server6-").append(tabname).append("=").append(QString::number(zkruncount,10));
            text_stream5 << logmessage << "\r\n";
            file.flush();
            file.close();

            //第一次核对不平，重新执行bcp
            int checkcount = 0;
            while(checkcount < 5 && sixthruncount != zkruncount)
            {
                checkcount++;
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream6(&file);
                logmessage = "server6:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("数据核对不平:").append(sql_sixthruncheck);
                logmessage.append(" server6-").append(tabname).append("=").append(QString::number(sixthruncount,10));
                logmessage.append(" zk-server6-").append(tabname).append("=").append(QString::number(zkruncount,10));
                text_stream6 << logmessage << "\r\n";
                logmessage = "开始下一遍bcp...";
                text_stream6 << logmessage << "\r\n";
                file.flush();
                file.close();
                QString removepath = Server6Path;
                removepath.append("\\").append(tabname).append(".bcp");
                QFile::remove(removepath);

                QProcess processagain(this);
                processagain.start(cmd);
                processagain.waitForFinished(-1);
                bool result = false;
                while(!result)
                {
                    result = zkqueryrun.exec(sql_orderrec);
                }
                if(result)
                {
                    db1.commit();
                    QProcess processinagain(this);
                    processinagain.start(cmdin);
                    processinagain.waitForFinished(-1);
                }
                else
                {
                    db1.rollback();
                }

                sixthqueryrun.exec(sql_sixthruncheck);
                if(sixthqueryrun.next())
                {
                    sixthruncount = sixthqueryrun.value(0).toInt();
                }
                zkqueryrun.exec(sql_sixthruncheck);
                if(zkqueryrun.next())
                {
                    zkruncount = zkqueryrun.value(0).toInt();
                }
            }
            if(sixthruncount != zkruncount)
            {
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream7(&file);
                logmessage = "server6:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("请人工处理不平数据:").append(sql_sixthruncheck);
                logmessage.append(" server6-").append(tabname).append("=").append(QString::number(sixthruncount,10));
                logmessage.append(" zk-server6-").append(tabname).append("=").append(QString::number(zkruncount,10));
                text_stream7 << logmessage << "\r\n";
                file.flush();
                file.close();
                emit showDiff("节点六",tabname,sixthruncount,zkruncount,sixthruncount-zkruncount);
            }

        }

        for(int k=0; k<his_count; k++)
        {
            progress_count++;
            progress = (progress_count/float((trade_count+his_count)*2))*100;
            QString tabname = HisList[k];
            QString fieldname = HisFieldList[k];
            QString sql = "select * from his..";
            sql.append(tabname);
            sql.append(" where serverid = '6'");
            if(fieldname != "-1")
            {
                sql.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            QProcess processhis(this);
            QString cmd = "bcp \"";
            cmd.append(sql);
            cmd.append("\"");
            cmd.append(" queryout ");
            cmd.append(Server6Path);
            cmd.append("\\");
            cmd.append(tabname);
            cmd.append(".bcp  -S");
            cmd.append(hisserver_ip[6]);
            cmd.append(",");
            cmd.append(hisserver_port[6]);
            cmd.append(" -U");
            cmd.append(hisserver_user[6]);
            cmd.append(" -P");
            cmd.append(hisserver_password[6]);
            cmd.append(" -n");
            QString cmdin = "bcp his..";
            cmdin.append(tabname);
            cmdin.append(" in ");
            cmdin.append(Server6Path);
            cmdin.append("\\");
            cmdin.append(tabname);
            cmdin.append(".bcp  -S");
            cmdin.append(hisserver_ip[0]);
            cmdin.append(",");
            cmdin.append(hisserver_port[0]);
            cmdin.append(" -U");
            cmdin.append(hisserver_user[0]);
            cmdin.append(" -P");
            cmdin.append(hisserver_password[0]);
            cmdin.append(" -n");
            begintime = starttime6->currentDateTime().toString("yyyy.MM.dd hh:mm:ss");
            processhis.start(cmd);
            file.open(QIODevice::WriteOnly | QIODevice::Append);
            QTextStream text_stream8(&file);
            logmessage = "server6:";
            logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append("bcp out table ").append(tabname).append(":").append(sql);
            text_stream8 << logmessage << "\r\n";
            file.flush();
            file.close();
            emit showNum(6,progress,1,tabname,begintime);//触发信号
            processhis.waitForFinished(-1);

            QString sql_orderrec="delete  from his..";
            sql_orderrec.append(tabname);
            sql_orderrec.append(" where serverid = '6'");
            if(fieldname != "-1")
            {
                sql_orderrec.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            bool result = false;
            while(!result)
            {
                result = zkqueryhis.exec(sql_orderrec);
            }
            if(result)
            {
                db2.commit();
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream9(&file);
                logmessage = "server6:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append(sql_orderrec);
                text_stream9 << logmessage << "\r\n";
                file.flush();
                file.close();

                progress_count++;
                progress = (progress_count/float((trade_count+his_count)*2))*100;
                QProcess processin(this);

                begintime = starttime6->currentDateTime().toString("yyyy.MM.dd hh:mm:ss");
                processin.start(cmdin);
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream11(&file);
                logmessage = "server6:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":").append("bcp in ").append(tabname);
                text_stream11 << logmessage << "\r\n";
                file.flush();
                file.close();
                emit showNum(6,progress,2,tabname,begintime);//触发信号
                processin.waitForFinished(-1);
            }
            else
            {
                db2.rollback();
                QString me = "节点六删除表";
                me.append(tabname).append("失败").append(zkqueryhis.lastError().databaseText());
                emit exceptionsignal(me);
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream10(&file);
                logmessage = "server6:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append(":回滚:").append(sql_orderrec);
                text_stream10 << logmessage << "\r\n";
                file.flush();
                file.close();
            }


            //进行数据核对
            int zkhiscount = 0;
            int sixthhiscount = 0;
            QString sql_sixthhischeck="select count(*)  from his..";
            sql_sixthhischeck.append(tabname);
            sql_sixthhischeck.append(" WITH(NOLOCK) where serverid = '6'");
            if(fieldname != "-1")
            {
                sql_sixthhischeck.append(" and ").append(fieldname).append(" = ").append(this->dt);
            }
            sixthqueryhis.exec(sql_sixthhischeck);
            if(sixthqueryhis.next())
            {
                sixthhiscount = sixthqueryhis.value(0).toInt();
            }

            zkqueryhis.exec(sql_sixthhischeck);
            if(zkqueryhis.next())
            {
                zkhiscount = zkqueryhis.value(0).toInt();
            }

            file.open(QIODevice::WriteOnly | QIODevice::Append);
            QTextStream text_stream12(&file);
            logmessage = "server6:";
            logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("数据核对:").append(sql_sixthhischeck);
            logmessage.append(" server6-").append(tabname).append("=").append(QString::number(sixthhiscount,10));
            logmessage.append(" zk-server6-").append(tabname).append("=").append(QString::number(zkhiscount,10));
            text_stream12 << logmessage << "\r\n";
            file.flush();
            file.close();

            //第一次核对不平，重新执行bcp
            int checkcount = 0;
            while(checkcount < 5 && sixthhiscount != zkhiscount)
            {
                checkcount++;
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream13(&file);
                logmessage = "server6:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("数据核对不平:").append(sql_sixthhischeck);
                logmessage.append(" server6-").append(tabname).append("=").append(QString::number(sixthhiscount,10));
                logmessage.append(" zk-server6-").append(tabname).append("=").append(QString::number(zkhiscount,10));
                text_stream13 << logmessage << "\r\n";
                logmessage = "开始下一遍bcp...";
                text_stream13 << logmessage << "\r\n";
                file.flush();
                file.close();
                QString removepath = Server6Path;
                removepath.append("\\").append(tabname).append(".bcp");
                QFile::remove(removepath);
                QProcess processagain(this);
                processagain.start(cmd);
                processagain.waitForFinished(-1);
                bool result = false;
                while(!result)
                {
                    result = zkqueryhis.exec(sql_orderrec);
                }
                if(result)
                {
                    db1.commit();
                    QProcess processinagain(this);
                    processinagain.start(cmdin);
                    processinagain.waitForFinished(-1);
                }
                else
                {
                    db1.rollback();
                }

                sixthqueryhis.exec(sql_sixthhischeck);
                if(sixthqueryhis.next())
                {
                    sixthhiscount = sixthqueryhis.value(0).toInt();
                }
                zkqueryhis.exec(sql_sixthhischeck);
                if(zkqueryhis.next())
                {
                    zkhiscount = zkqueryhis.value(0).toInt();
                }
            }
            if(sixthhiscount != zkhiscount)
            {
                file.open(QIODevice::WriteOnly | QIODevice::Append);
                QTextStream text_stream14(&file);
                logmessage = "server6:";
                logmessage.append(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss")).append("请人工处理不平数据:").append(sql_sixthhischeck);
                logmessage.append(" server6-").append(tabname).append("=").append(QString::number(sixthhiscount,10));
                logmessage.append(" zk-server6-").append(tabname).append("=").append(QString::number(zkhiscount,10));
                text_stream14 << logmessage << "\r\n";
                file.flush();
                file.close();
                emit showDiff("节点六",tabname,sixthhiscount,zkhiscount,sixthhiscount-zkhiscount);
            }
        }

        db1.close();
        db2.close();
        db3.close();
        db4.close();

        file.open(QIODevice::WriteOnly | QIODevice::Append);
        QTextStream text_stream514(&file);
        QString logmessage5;
        logmessage5 = "节点六同步完成";
        text_stream514 << logmessage5 << "\r\n";
        file.flush();
        file.close();

        emit updatesignal();
    } catch(std::exception& e)
    {
        emit exceptionsignal(e.what());
    }
}
worker::~worker()
{

}
