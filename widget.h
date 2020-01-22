#ifndef WIDGET_H
#define WIDGET_H
#include <QStandardItemModel>
#include <QWidget>
#include <QSqlDatabase>
#include <QSqlError>
#include <QVector>
#include <QMap>
#include "worker.h"
#include <QTextEdit>
#include <QProgressBar>
#include <QColor>
namespace Ui {
class Widget;
}

class Widget : public QWidget
{
    Q_OBJECT

public:
    explicit Widget(QWidget *parent = 0);
    QStandardItemModel* _model;
 //   QSqlDatabase jzjydb, jzjyhis1db,jzjyhis2db,jzjyhis3db,jzjyhis4db,jzjyhis5db,jzjyhis6db;
    ~Widget();
    void stringToHtmlFilter(QString &str);
    void stringToHtml(QString &str,QColor crl);

signals:

private slots:
    void on_pushButton_clicked();
    void DisplayMsg(int k,float i,int j,QString tablename,QString begintime);
    void DisplayDiff(QString server,QString table,int his,int zk, int diff);
    void updateslot();
    void exceptionslot(QString e);

private:
    Ui::Widget *ui;
    worker *myTFirst;
    worker *myTSecond;
    worker *myTThird;
    worker *myTFourth;
    worker *myTFifth;
    worker *myTSixth;
    QThread *threadfirst;
    QThread *threadsecond;
    QThread *threadthird;
    QThread *threadfourth;
    QThread *threadfifth;
    QThread *threadsixth;
public:
    QString dt;
    static int finishthread;
    static int checkthread;
    static int rownum;
    bool check1 = true;
    bool check2 = true;
    bool check3 = true;
    bool check4 = true;
    bool check5 = true;
    bool check6 = true;
    QTextEdit * tablename1 = new QTextEdit();
    QTextEdit * tablename2 = new QTextEdit();
    QTextEdit * tablename3 = new QTextEdit();
    QTextEdit * tablename4 = new QTextEdit();
    QTextEdit * tablename5 = new QTextEdit();
    QTextEdit * tablename6 = new QTextEdit();
    QProgressBar *probar1 = new QProgressBar();
    QProgressBar *probar2 = new QProgressBar();
    QProgressBar *probar3 = new QProgressBar();
    QProgressBar *probar4 = new QProgressBar();
    QProgressBar *probar5 = new QProgressBar();
    QProgressBar *probar6 = new QProgressBar();
    QTextEdit * timeshow1 = new QTextEdit();
    QTextEdit * timeshow2 = new QTextEdit();
    QTextEdit * timeshow3 = new QTextEdit();
    QTextEdit * timeshow4 = new QTextEdit();
    QTextEdit * timeshow5 = new QTextEdit();
    QTextEdit * timeshow6 = new QTextEdit();

    QDateTime * starttime1 = new QDateTime();
    QDateTime * starttime2 = new QDateTime();
    QDateTime * starttime3 = new QDateTime();
    QDateTime * starttime4 = new QDateTime();
    QDateTime * starttime5 = new QDateTime();
    QDateTime * starttime6 = new QDateTime();

};

#endif // WIDGET_H
