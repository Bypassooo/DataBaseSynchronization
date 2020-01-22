#include "widget.h"
#include "ui_widget.h"
#include <QDebug>
#include <QMessageBox>
#include <QSqlQuery>
#include <QSqlRecord>
#include <QMap>
#include <QSettings>
#include <QVector>
#include <QDate>
int Widget::finishthread = 0;
int Widget::checkthread = 0;
int Widget::rownum = 0;

Widget::Widget(QWidget *parent) :
    QWidget(parent),
    ui(new Ui::Widget)
{
    ui->setupUi(this);
    setWindowIcon(QIcon(":/images/synlogo.png"));
    ui->tableWidget->setColumnCount(5);
    ui->tableWidget->setRowCount(7);
    ui->tableWidget->setHorizontalHeaderLabels(QStringList()<<"选择"<<"节点"<<"进度"<<"当前处理表"<<"开始时间");
    ui->tableWidget->verticalHeader()->setVisible(false);// 隐藏最左边一列
    ui->tableWidget->setColumnWidth(0,50);
    ui->tableWidget->setColumnWidth(1,100);
    ui->tableWidget->setColumnWidth(2,209);
    ui->tableWidget->setColumnWidth(3,250);
    ui->tableWidget->setColumnWidth(4,150);
    ui->tableWidget->setRowHeight(0,6);
    //设置默认日期为当前日期
    ui->dateEdit->setDate(QDate::currentDate());
    //第一列设置
    QTableWidgetItem *check_flag1 = new QTableWidgetItem();
    check_flag1->setCheckState(Qt::Checked);
    ui->tableWidget->setItem(1,0,check_flag1);

    QTableWidgetItem *check_flag2 = new QTableWidgetItem();
    check_flag2->setCheckState(Qt::Checked);
    ui->tableWidget->setItem(2,0,check_flag2);

    QTableWidgetItem *check_flag3 = new QTableWidgetItem();
    check_flag3->setCheckState(Qt::Checked);
    ui->tableWidget->setItem(3,0,check_flag3);

    QTableWidgetItem *check_flag4 = new QTableWidgetItem();
    check_flag4->setCheckState(Qt::Checked);
    ui->tableWidget->setItem(4,0,check_flag4);

    QTableWidgetItem *check_flag5 = new QTableWidgetItem();
    check_flag5->setCheckState(Qt::Checked);
    ui->tableWidget->setItem(5,0,check_flag5);

    QTableWidgetItem *check_flag6 = new QTableWidgetItem();
    check_flag6->setCheckState(Qt::Checked);
    ui->tableWidget->setItem(6,0,check_flag6);

    //第二列设置
    QTextEdit * servername1 = new QTextEdit();
    servername1->setText("节点一");
    ui->tableWidget->setCellWidget(1,1,servername1);
    servername1->setReadOnly(true);

    QTextEdit * servername2 = new QTextEdit();
    servername2->setText("节点二");
    ui->tableWidget->setCellWidget(2,1,servername2);
    servername2->setReadOnly(true);

    QTextEdit * servername3 = new QTextEdit();
    servername3->setText("节点三");
    ui->tableWidget->setCellWidget(3,1,servername3);
    servername3->setReadOnly(true);

    QTextEdit * servername4 = new QTextEdit();
    servername4->setText("节点四");
    ui->tableWidget->setCellWidget(4,1,servername4);
    servername4->setReadOnly(true);

    QTextEdit * servername5 = new QTextEdit();
    servername5->setText("节点五");
    ui->tableWidget->setCellWidget(5,1,servername5);
    servername5->setReadOnly(true);

    QTextEdit * servername6 = new QTextEdit();
    servername6->setText("节点六");
    ui->tableWidget->setCellWidget(6,1,servername6);
    servername6->setReadOnly(true);

    //第三列
    probar1->setStyleSheet(
                "QProgressBar {border: 2px solid grey;   border-radius: 5px;"
                "background-color: #FFFFFF;"
                "text-align: center;}"
                );
    probar1->setValue(0);
    ui->tableWidget->setCellWidget(1,2,probar1);

    probar2->setStyleSheet(
                "QProgressBar {border: 2px solid grey;   border-radius: 5px;"
                "background-color: #FFFFFF;"
                "text-align: center;}"
                );
    probar2->setValue(0);
    ui->tableWidget->setCellWidget(2,2,probar2);

    probar3->setStyleSheet(
                "QProgressBar {border: 2px solid grey;   border-radius: 5px;"
                "background-color: #FFFFFF;"
                "text-align: center;}"
                );
    probar3->setValue(0);
    ui->tableWidget->setCellWidget(3,2,probar3);

    probar4->setStyleSheet(
                "QProgressBar {border: 2px solid grey;   border-radius: 5px;"
                "background-color: #FFFFFF;"
                "text-align: center;}"
                );
    probar4->setValue(0);
    ui->tableWidget->setCellWidget(4,2,probar4);

    probar5->setStyleSheet(
                "QProgressBar {border: 2px solid grey;   border-radius: 5px;"
                "background-color: #FFFFFF;"
                "text-align: center;}"
                );
    probar5->setValue(0);
    ui->tableWidget->setCellWidget(5,2,probar5);

    probar6->setStyleSheet(
                "QProgressBar {border: 2px solid grey;   border-radius: 5px;"
                "background-color: #FFFFFF;"
                "text-align: center;}"
                );
    probar6->setValue(0);
    ui->tableWidget->setCellWidget(6,2,probar6);

    tablename1->setText("即将开始节点一数据同步");
    ui->tableWidget->setCellWidget(1,3,tablename1);
    tablename1->setReadOnly(true);

    tablename2->setText("即将开始节点二数据同步");
    ui->tableWidget->setCellWidget(2,3,tablename2);
    tablename2->setReadOnly(true);

    tablename3->setText("即将开始节点三数据同步");
    ui->tableWidget->setCellWidget(3,3,tablename3);
    tablename3->setReadOnly(true);

    tablename4->setText("即将开始节点四数据同步");
    ui->tableWidget->setCellWidget(4,3,tablename4);
    tablename4->setReadOnly(true);

    tablename5->setText("即将开始节点五数据同步");
    ui->tableWidget->setCellWidget(5,3,tablename5);
    tablename5->setReadOnly(true);

    tablename6->setText("即将开始节点六数据同步");
    ui->tableWidget->setCellWidget(6,3,tablename6);
    tablename6->setReadOnly(true);

    timeshow1->setText(starttime1->currentDateTime().toString("yyyy.MM.dd hh:mm:ss"));
    ui->tableWidget->setCellWidget(1,4,timeshow1);
    timeshow1->setReadOnly(true);

    timeshow2->setText(starttime2->currentDateTime().toString("yyyy.MM.dd hh:mm:ss"));
    ui->tableWidget->setCellWidget(2,4,timeshow2);
    timeshow2->setReadOnly(true);

    timeshow3->setText(starttime3->currentDateTime().toString("yyyy.MM.dd hh:mm:ss"));
    ui->tableWidget->setCellWidget(3,4,timeshow3);
    timeshow3->setReadOnly(true);

    timeshow4->setText(starttime4->currentDateTime().toString("yyyy.MM.dd hh:mm:ss"));
    ui->tableWidget->setCellWidget(4,4,timeshow4);
    timeshow4->setReadOnly(true);

    timeshow5->setText(starttime5->currentDateTime().toString("yyyy.MM.dd hh:mm:ss"));
    ui->tableWidget->setCellWidget(5,4,timeshow5);
    timeshow5->setReadOnly(true);

    timeshow6->setText(starttime6->currentDateTime().toString("yyyy.MM.dd hh:mm:ss"));
    ui->tableWidget->setCellWidget(6,4,timeshow6);
    timeshow6->setReadOnly(true);

    ui->textBrowser->setText("...");

    ui->tableWidget_2->setColumnCount(5);
    ui->tableWidget_2->setHorizontalHeaderLabels(QStringList()<<"节点"<<"表名"<<"历史库"<<"总控库"<<"差值");
    ui->tableWidget_2->verticalHeader()->setVisible(false);// 隐藏最左边一列
    ui->tableWidget_2->setColumnWidth(0,100);
    ui->tableWidget_2->setColumnWidth(1,179);
    ui->tableWidget_2->setColumnWidth(2,160);
    ui->tableWidget_2->setColumnWidth(3,160);
    ui->tableWidget_2->setColumnWidth(4,160);
    ui->tableWidget_2->setRowHeight(0,6);
    //获取用户选择的日期
    dt = ui->dateEdit->date().toString("yyyyMMdd");
}

Widget::~Widget()
{
 //   jzjydb.close();
    delete ui;
}

void Widget::stringToHtmlFilter(QString &str)
{
   //注意这几行代码的顺序不能乱，否则会造成多次替换
   str.replace("&","&amp;");
   str.replace(">","&gt;");
   str.replace("<","&lt;");
   str.replace("\"","&quot;");
   str.replace("\'","&#39;");
   str.replace(" ","&nbsp;");
   str.replace("\n","<br>");
   str.replace("\r","<br>");
}
void Widget::stringToHtml(QString &str,QColor crl)
{
 QByteArray array;
 array.append(crl.red());
 array.append(crl.green());
 array.append(crl.blue());
 QString strC(array.toHex());
 str = QString("<span style=\" color:#%1;\">%2</span>").arg(strC).arg(str);
}
void Widget::exceptionslot(QString e)
{
    QColor  clrR(255,0,0);
    stringToHtmlFilter(e);
    stringToHtml(e,clrR);
    ui->textBrowser->insertHtml(e);
    QColor  clrRend(0,0,0);
    QString end = "\n";
    stringToHtmlFilter(end);
    stringToHtml(end,clrRend);
    ui->textBrowser->insertHtml(end);
}
void Widget::DisplayDiff(QString server, QString table, int his, int zk, int diff)
{
    ui->tableWidget_2->setRowCount(rownum+1);
    QTableWidgetItem *check_flag21 = new QTableWidgetItem(server);
    ui->tableWidget_2->setItem(rownum,0,check_flag21);
    QTableWidgetItem *check_flag22 = new QTableWidgetItem(table);
    ui->tableWidget_2->setItem(rownum,1,check_flag22);
    QTableWidgetItem *check_flag23 = new QTableWidgetItem(QString::number(his, 10));
    ui->tableWidget_2->setItem(rownum,2,check_flag23);
    QTableWidgetItem *check_flag24 = new QTableWidgetItem(QString::number(zk, 10));
    ui->tableWidget_2->setItem(rownum,3,check_flag24);
    QTableWidgetItem *check_flag25 = new QTableWidgetItem(QString::number(diff, 10));
    ui->tableWidget_2->setItem(rownum,4,check_flag25);
    rownum++;

}

void Widget::DisplayMsg(int k,float i,int j,QString tablename,QString begintime)
{
    if(k ==1)
    {
        //当前处理表
        QString showname;
        if(j == 1)
        {
            showname = "正在导出表";
        }
        else
        {
            showname = "正在导入表";
        }
        showname.append(tablename);
        showname.append("...");
        if(i == 100)
            showname = "同步完成";
        tablename1->setReadOnly(false);
        tablename1->setText(showname);
        ui->tableWidget->setCellWidget(1,3,tablename1);
        tablename1->setReadOnly(true);
        //进度条
        probar1->setStyleSheet(
                    "QProgressBar {border: 2px solid grey;   border-radius: 5px;"
                    "background-color: #FFFFFF;"
                    "text-align: center;}"
                    );
        probar1->setValue(i);
        ui->tableWidget->setCellWidget(1,2,probar1);
        //开始时间
        timeshow1->setReadOnly(false);
        timeshow1->setText(begintime);
        ui->tableWidget->setCellWidget(1,4,timeshow1);
        timeshow1->setReadOnly(true);
        QString logtext = "节点一";
        logtext.append(showname);
        ui->textBrowser->append(logtext);
    }
    else if(k == 2)
    {
        //当前处理表
        QString showname;
        if(j == 1)
        {
            showname = "正在导出表";
        }
        else
        {
            showname = "正在导入表";
        }
        showname.append(tablename);
        showname.append("...");
        if(i == 100)
            showname = "同步完成";
        tablename2->setReadOnly(false);
        tablename2->setText(showname);
        ui->tableWidget->setCellWidget(2,3,tablename2);
        tablename2->setReadOnly(true);
        //进度条
        probar2->setStyleSheet(
                    "QProgressBar {border: 2px solid grey;   border-radius: 5px;"
                    "background-color: #FFFFFF;"
                    "text-align: center;}"
                    );
        probar2->setValue(i);
        ui->tableWidget->setCellWidget(2,2,probar2);
        //开始时间
        timeshow2->setReadOnly(false);
        timeshow2->setText(begintime);
        ui->tableWidget->setCellWidget(2,4,timeshow2);
        timeshow2->setReadOnly(true);
        QString logtext = "节点二";
        logtext.append(showname);
        ui->textBrowser->append(logtext);
    }
    else if(k == 3)
    {
        //当前处理表
        QString showname;
        if(j == 1)
        {
            showname = "正在导出表";
        }
        else
        {
            showname = "正在导入表";
        }
        showname.append(tablename);
        showname.append("...");
        if(i == 100)
            showname = "同步完成";
        tablename3->setReadOnly(false);
        tablename3->setText(showname);
        ui->tableWidget->setCellWidget(3,3,tablename3);
        tablename3->setReadOnly(true);
        //进度条
        probar3->setStyleSheet(
                    "QProgressBar {border: 2px solid grey;   border-radius: 5px;"
                    "background-color: #FFFFFF;"
                    "text-align: center;}"
                    );
        probar3->setValue(i);
        ui->tableWidget->setCellWidget(3,2,probar3);
        //开始时间
        timeshow3->setReadOnly(false);
        timeshow3->setText(begintime);
        ui->tableWidget->setCellWidget(3,4,timeshow3);
        timeshow3->setReadOnly(true);
        QString logtext = "节点三";
        logtext.append(showname);
        ui->textBrowser->append(logtext);
    }
    else if(k == 4)
    {
        //当前处理表
        QString showname;
        if(j == 1)
        {
            showname = "正在导出表";
        }
        else
        {
            showname = "正在导入表";
        }
        showname.append(tablename);
        showname.append("...");
        if(i == 100)
            showname = "同步完成";
        tablename4->setReadOnly(false);
        tablename4->setText(showname);
        ui->tableWidget->setCellWidget(4,3,tablename4);
        tablename4->setReadOnly(true);
        //进度条
        probar4->setStyleSheet(
                    "QProgressBar {border: 2px solid grey;   border-radius: 5px;"
                    "background-color: #FFFFFF;"
                    "text-align: center;}"
                    );
        probar4->setValue(i);
        ui->tableWidget->setCellWidget(4,2,probar4);
        //开始时间
        timeshow4->setReadOnly(false);
        timeshow4->setText(begintime);
        ui->tableWidget->setCellWidget(4,4,timeshow4);
        timeshow4->setReadOnly(true);
        QString logtext = "节点四";
        logtext.append(showname);
        ui->textBrowser->append(logtext);
    }
    else if(k == 5)
    {
        //当前处理表
        QString showname;
        if(j == 1)
        {
            showname = "正在导出表";
        }
        else
        {
            showname = "正在导入表";
        }
        showname.append(tablename);
        showname.append("...");
        if(i == 100)
            showname = "同步完成";
        tablename5->setReadOnly(false);
        tablename5->setText(showname);
        ui->tableWidget->setCellWidget(5,3,tablename5);
        tablename5->setReadOnly(true);
        //进度条
        probar5->setStyleSheet(
                    "QProgressBar {border: 2px solid grey;   border-radius: 5px;"
                    "background-color: #FFFFFF;"
                    "text-align: center;}"
                    );
        probar5->setValue(i);
        ui->tableWidget->setCellWidget(5,2,probar5);
        //开始时间
        timeshow5->setReadOnly(false);
        timeshow5->setText(begintime);
        ui->tableWidget->setCellWidget(5,4,timeshow5);
        timeshow5->setReadOnly(true);
        QString logtext = "节点五";
        logtext.append(showname);
        ui->textBrowser->append(logtext);
    }
    else if(k == 6)
    {
        //当前处理表
        QString showname;
        if(j == 1)
        {
            showname = "正在导出表";
        }
        else
        {
            showname = "正在导入表";
        }
        showname.append(tablename);
        showname.append("...");
        if(i == 100)
            showname = "同步完成";
        tablename6->setReadOnly(false);
        tablename6->setText(showname);
        ui->tableWidget->setCellWidget(6,3,tablename6);
        tablename6->setReadOnly(true);
        //进度条
        probar6->setStyleSheet(
                    "QProgressBar {border: 2px solid grey;   border-radius: 5px;"
                    "background-color: #FFFFFF;"
                    "text-align: center;}"
                    );
        probar6->setValue(i);
        ui->tableWidget->setCellWidget(6,2,probar6);
        //开始时间
        timeshow6->setReadOnly(false);
        timeshow6->setText(begintime);
        ui->tableWidget->setCellWidget(6,4,timeshow6);
        timeshow1->setReadOnly(true);
        QString logtext = "节点六";
        logtext.append(showname);
        ui->textBrowser->append(logtext);
    }
}

void Widget::updateslot()
{
    finishthread++;
    if(finishthread == checkthread)
    {
        ui->pushButton->setEnabled(true);
    }
}

//使用bcp进行数据库复制
void Widget::on_pushButton_clicked()
{
    checkthread = 0;
    if(ui->tableWidget->item(1,0)->checkState() == Qt::Checked)
    {
        check1 = true;
        checkthread++;
    }
    else
    {
        check1 = false;
    }
    if(ui->tableWidget->item(2,0)->checkState() == Qt::Checked)
    {
        check2 = true;
        checkthread++;
    }
    else
    {
        check2 = false;
    }
    if(ui->tableWidget->item(3,0)->checkState() == Qt::Checked)
    {
        check3 = true;
        checkthread++;
    }
    else
    {
        check3 = false;
    }
    if(ui->tableWidget->item(4,0)->checkState() == Qt::Checked)
    {
        check4 = true;
        checkthread++;
    }
    else
    {
        check4 = false;
    }
    if(ui->tableWidget->item(5,0)->checkState() == Qt::Checked)
    {
        check5 = true;
        checkthread++;
    }
    else
    {
        check5 = false;
    }
    if(ui->tableWidget->item(6,0)->checkState() == Qt::Checked)
    {
        check6 = true;
        checkthread++;
    }
    else
    {
        check6 = false;
    }
    //获取日期
    QString chosedate = ui->dateEdit->date().toString("yyyyMMdd");
    ui->pushButton->setEnabled(false);
    threadfirst = new QThread;
    threadsecond = new QThread;
    threadthird = new QThread;
    threadfourth = new QThread;
    threadfifth = new QThread;
    threadsixth = new QThread;
    myTFirst = new worker;
    myTSecond = new worker;
    myTThird = new worker;
    myTFourth = new worker;
    myTFifth = new worker;
    myTSixth = new worker;
    myTFirst->dt = chosedate;
    myTSecond->dt = chosedate;
    myTThird->dt = chosedate;
    myTFourth->dt = chosedate;
    myTFifth->dt = chosedate;
    myTSixth->dt = chosedate;
    myTFirst->moveToThread(threadfirst);
    myTSecond->moveToThread(threadsecond);
    myTThird->moveToThread(threadthird);
    myTFourth->moveToThread(threadfourth);
    myTFifth->moveToThread(threadfifth);
    myTSixth->moveToThread(threadsixth);
    if(this->check1)
    {
        connect(threadfirst,SIGNAL(started()),myTFirst,SLOT(DeDabaFirst()));//新建的线程开始后执行DeDabaFirst函数
        connect(myTFirst, SIGNAL(showNum(int,float,int,QString,QString)), this, SLOT(DisplayMsg(int,float,int,QString,QString)));//子线程触发的信号传递了值进来
        connect(myTFirst, SIGNAL(updatesignal()), this, SLOT(updateslot()));
        connect(myTFirst, SIGNAL(exceptionsignal(QString)), this, SLOT(exceptionslot(QString)));
        connect(myTFirst, SIGNAL(showDiff(QString,QString,int,int,int)), this, SLOT(DisplayDiff(QString,QString,int,int,int)));
        threadfirst->start();
    }
    if(this->check2)
    {
        connect(threadsecond,SIGNAL(started()),myTSecond,SLOT(DeDabaSecond()));//新建第二个线程执行DeDabaSecond函数
        connect(myTSecond, SIGNAL(showNum(int,float,int,QString,QString)), this, SLOT(DisplayMsg(int,float,int,QString,QString)));//子线程触发的信号传递了值进来
        connect(myTSecond, SIGNAL(updatesignal()), this, SLOT(updateslot()));
        connect(myTSecond, SIGNAL(exceptionsignal(QString)), this, SLOT(exceptionslot(QString)));
        connect(myTSecond, SIGNAL(showDiff(QString,QString,int,int,int)), this, SLOT(DisplayDiff(QString,QString,int,int,int)));
        threadsecond->start();
    }
    if(this->check3)
    {
        connect(threadthird,SIGNAL(started()),myTThird,SLOT(DeDabaThird()));//新建第二个线程执行DeDabaSecond函数
        connect(myTThird, SIGNAL(showNum(int,float,int,QString,QString)), this, SLOT(DisplayMsg(int,float,int,QString,QString)));//子线程触发的信号传递了值进来
        connect(myTThird, SIGNAL(updatesignal()), this, SLOT(updateslot()));
        connect(myTThird, SIGNAL(exceptionsignal(QString)), this, SLOT(exceptionslot(QString)));
        connect(myTThird, SIGNAL(showDiff(QString,QString,int,int,int)), this, SLOT(DisplayDiff(QString,QString,int,int,int)));
        threadthird->start();
    }
    if(this->check4)
    {
        connect(threadfourth,SIGNAL(started()),myTFourth,SLOT(DeDabaFourth()));//新建第二个线程执行DeDabaSecond函数
        connect(myTFourth, SIGNAL(showNum(int,float,int,QString,QString)), this, SLOT(DisplayMsg(int,float,int,QString,QString)));
        connect(myTFourth, SIGNAL(updatesignal()), this, SLOT(updateslot()));
        connect(myTFourth, SIGNAL(exceptionsignal(QString)), this, SLOT(exceptionslot(QString)));
        connect(myTFourth, SIGNAL(showDiff(QString,QString,int,int,int)), this, SLOT(DisplayDiff(QString,QString,int,int,int)));
        threadfourth->start();
    }
    if(this->check5)
    {
        connect(threadfifth,SIGNAL(started()),myTFifth,SLOT(DeDabaFifth()));//新建第二个线程执行DeDabaSecond函数
        connect(myTFifth, SIGNAL(showNum(int,float,int,QString,QString)), this, SLOT(DisplayMsg(int,float,int,QString,QString)));
        connect(myTFifth, SIGNAL(updatesignal()), this, SLOT(updateslot()));
        connect(myTFifth, SIGNAL(exceptionsignal(QString)), this, SLOT(exceptionslot(QString)));
        connect(myTFifth, SIGNAL(showDiff(QString,QString,int,int,int)), this, SLOT(DisplayDiff(QString,QString,int,int,int)));
        threadfifth->start();
    }
    if(this->check6)
    {
        connect(threadsixth,SIGNAL(started()),myTSixth,SLOT(DeDabaSixth()));//新建第二个线程执行DeDabaSecond函数
        connect(myTSixth, SIGNAL(showNum(int,float,int,QString,QString)), this, SLOT(DisplayMsg(int,float,int,QString,QString)));
        connect(myTSixth, SIGNAL(updatesignal()), this, SLOT(updateslot()));
        connect(myTSixth, SIGNAL(exceptionsignal(QString)), this, SLOT(exceptionslot(QString)));
        connect(myTSixth, SIGNAL(showDiff(QString,QString,int,int,int)), this, SLOT(DisplayDiff(QString,QString,int,int,int)));
        threadsixth->start();
    }
}
