#include <QDebug>
#include <QSqlError>
#include <qsqlquery.h>
#include <QSqlDriver>
#include <QMutex>
#include <QThread>
#include <QSqlRecord>
#include "dbinterfaceais.h"
#include <QtMath>
#include <QTimer>
#include <QCoreApplication>
#include <QDateTime>

DBInterfaceAIS::DBInterfaceAIS(QMutex *mutex, StructDBInfo structDBInfo, QObject *parent)
    : QObject(parent)
{
    db=NULL;
    query1=NULL;
    timerCheckConnection=new QTimer(this);
    connect(timerCheckConnection,SIGNAL(timeout()),
            this,SLOT(slotTimerEventCheckConnection()));
    timerCheckConnection->start(3600000);//一小时检查一次
    this->mutex=mutex;
    dbInfo=structDBInfo;
}

bool DBInterfaceAIS::connectToDB()
{
    /*****connect to source database***/
    QMutexLocker locker(mutex);
    db = new QSqlDatabase(QSqlDatabase::addDatabase(dbInfo.type,dbInfo.connectionName));

    db->setHostName(dbInfo.hostIP);
    db->setDatabaseName(dbInfo.dbName); //Oracle的SID
    db->setUserName(dbInfo.userName);
    db->setPassword(dbInfo.pwd);

    if(!db->open())
    {
        emit sigShowInfo("Can't connect to database! "+db->lastError().text()
                         +" Connection name is "+db->connectionName());
        qDebug()<<"Can't connect to database! "+db->lastError().text()
                  +" Connection name is "+db->connectionName();
        return false;
    }
    else
    {
        emit sigShowInfo("Database is connected successfully! Connection name is "+db->connectionName());
        qDebug()<<"Database is connected successfully! Connection name is "+db->connectionName();
        query1=new QSqlQuery(*db);
        query1->setForwardOnly(true);
        return true;
    }
}

void DBInterfaceAIS::slotTimerEventCheckConnection()
{
    qint64 reTryConnectTimes=0;
    qDebug()<<"Checking connection with MySQL...";
    while(!checkConnection()) //检测连接是否还在
    {
        reTryConnectTimes++;
        emit sigShowInfo("Connection to source database  failed!"+db->lastError().text()
                         +" COnnection name is "+db->connectionName()
                         +" This therad will sleep "+QByteArray::number(pow(4,reTryConnectTimes))+" seconds");
        qDebug()<<"Connection to source database  failed!"+db->lastError().text()
                  +" COnnection name is "+db->connectionName()
                  +" This therad will sleep "+QByteArray::number(pow(4,reTryConnectTimes))+" seconds";
        this->thread()->sleep(pow(4,reTryConnectTimes)); //等待
    }
    qDebug()<<"Connection with MySQL is ok!";
}

//快速将大量数据插入表格。参数通过引用传递，减少内存占用
bool DBInterfaceAIS::quickInsertInBatch(QSqlQuery query, QString tableName,
                        QList<QVariantList> &listColumnData, QString insertMethod)
{
    qint64 reTryConnectTimes=0;
    while(!checkConnection()) //检测连接是否还在
    {
        reTryConnectTimes++;
        emit sigShowInfo("Connection to source database  failed!"+db->lastError().text()
                         +" COnnection name is "+db->connectionName()
                         +" This therad will sleep "+QByteArray::number(pow(4,reTryConnectTimes))+" seconds");
        qDebug()<<"Connection to source database  failed!"+db->lastError().text()
                  +" COnnection name is "+db->connectionName()
                  +" This therad will sleep "+QByteArray::number(pow(4,reTryConnectTimes))+" seconds";
        this->thread()->sleep(pow(4,reTryConnectTimes)); //等待
    }
    qint16 columnCount=listColumnData.size();
    if(columnCount<=0)
        return false;

    QString prepareSQL=insertMethod+" into "+tableName+" values (";
    for(int i=0;i<columnCount;i++)
        prepareSQL.append("?,");

    prepareSQL.chop(1); //去掉最后一个逗号
    prepareSQL.append(")");

    qint64 rowCount=listColumnData.first().size();//行数
    qint64 rowIndex=0;
    if(rowCount<=0)
        return false;

    while(rowIndex<rowCount)//一次插入太多会导致超出数据库的max_allowed_packet
    {
        //每次都要先prepare再addbindvalue，否则可能出错，只成功执行第一次操作。
        //经过测试，如果不这么做，当数据量少的时候没问题，数据量大的时候，会失败。
        if(!query.prepare(prepareSQL))
        {
            qDebug() <<prepareSQL<<". Error info:"<< query.lastError().text()<<endl;
            sigShowInfo(query.lastError().text());
            return false;
        }

        for(int indexColumn=0;indexColumn<columnCount;indexColumn++)
        {
            QVariantList listDataOfOneColumn=listColumnData.at(indexColumn)
                    .mid(rowIndex,ROWS_OF_SINGLE_BATCH);
            query.addBindValue(listDataOfOneColumn);
        }
        rowIndex+=ROWS_OF_SINGLE_BATCH;
        if (!query.execBatch())
        {
            qDebug() <<"Fail to execBatch:"<<prepareSQL<<query.lastError().text();
            sigShowInfo(query.lastError().text());
            return false;
        }
        if(rowIndex/ROWS_OF_SINGLE_BATCH%1==0)
        {
            qApp->processEvents();
            qDebug()<<QDateTime::currentDateTime()<<" Value of current rowIndex:"<<rowIndex;
        }
    }

    return true;
}

bool DBInterfaceAIS::insertOneRow(QSqlQuery query,QString tableName,QVariantList listData,QString insertMethod)
{
    qint16 columnCount=listData.size();
    if(columnCount<=0)
        return false;

    QString prepareSQL=insertMethod+" into "+tableName+" values (";
    for(int i=0;i<columnCount;i++)
        prepareSQL.append("?,");

    prepareSQL.chop(1); //去掉最后一个逗号
    prepareSQL.append(")");

    if(!query.prepare(prepareSQL))
    {
        qDebug() <<prepareSQL<<". Error info:"<< query.lastError().text()<<endl;
        sigShowInfo(query.lastError().text());
        return false;
    }

    QListIterator <QVariant> iListData(listData);
    while(iListData.hasNext())
    {
        query.addBindValue(iListData.next());
    }

    if (!query.exec())
    {
        qDebug() <<prepareSQL<<". Error info:"<<query.lastError();
        sigShowInfo(query.lastError().text());
        return false;
    }
    else
        return true;
}

QStringList DBInterfaceAIS::getSourceDBTablePartitions(QString tableName)
{
    QStringList listPartitions;

    qint64 reTryConnectTimes=0;
    while(!checkConnection()) //检测连接是否还在
    {
        reTryConnectTimes++;
        emit sigShowInfo("Connection to source database  failed!"+db->lastError().text()
                         +" COnnection name is "+db->connectionName()
                         +" This therad will sleep "+QByteArray::number(pow(4,reTryConnectTimes))+" seconds");
        qDebug()<<"Connection to source database  failed!"+db->lastError().text()
                  +" COnnection name is "+db->connectionName()
                  +" This therad will sleep "+QByteArray::number(pow(4,reTryConnectTimes))+" seconds";
        this->thread()->sleep(pow(4,reTryConnectTimes)); //等待
    }

    QSqlQuery query(*db);
    query.setForwardOnly(true);
    if(query.exec("explain partitions SELECT * FROM "+tableName))
    {
        if(query.next())
        {
            qint16 index=query.record().indexOf("partitions");
            if(index>=0)
            {
                return query.value(index).toString().split(',');
            }
        }
        return listPartitions;
    }
    else
    {
        emit sigShowInfo("Database error when explain partitions. "+query.lastError().text());
        return listPartitions;
    }
}


DBInterfaceAIS::~DBInterfaceAIS()
{
    if(db->isOpen())
    {
        db->close();
        delete db;
        db=NULL;
        QSqlDatabase::removeDatabase(dbInfo.connectionName);
    }
}

bool DBInterfaceAIS::hasFeatureOfTransaction()
{
    return db->driver()->hasFeature(QSqlDriver::Transactions);
}

bool DBInterfaceAIS::startTransaction()
{
    qint64 reTryConnectTimes=0;
    while(!checkConnection()) //检测连接是否还在
    {
        reTryConnectTimes++;
        emit sigShowInfo("Connection to source database  failed!"+db->lastError().text()
                         +" COnnection name is "+db->connectionName()
                         +" This therad will sleep "+QByteArray::number(pow(4,reTryConnectTimes))+" seconds");
        qDebug()<<"Connection to source database  failed!"+db->lastError().text()
                  +" COnnection name is "+db->connectionName()
                  +" This therad will sleep "+QByteArray::number(pow(4,reTryConnectTimes))+" seconds";
        this->thread()->sleep(pow(4,reTryConnectTimes)); //等待
    }
    return db->transaction();
}

bool DBInterfaceAIS::commitTransaction()
{
    return db->commit();
}

bool DBInterfaceAIS::checkConnection()
{
    if(db==NULL||db==NULL)
    {
        return false;
    }

    if(db->isOpen())
    {
        QString strSQL;
        //strSQL="select 1 from " TABLE_NAME_FOR_CON_CHECK " limit 1";
        strSQL="show tables";
        QSqlQuery queryTemp; //防止和别的query冲突，使用临时变量来查询
        if(queryTemp.exec(strSQL))
            return true;
        else
        {
            return reConnectToDB();
        }
    }
    else if(!db->open())
    {
        emit sigShowInfo("Can't connect to  database! Connection name is:"+db->connectionName()+
                         db->lastError().text());
        qDebug()<<"Can't connect to  database! Connection name is:"+db->connectionName()+
                  db->lastError().text();
        return reConnectToDB();

    }
    else
    {
        emit sigShowInfo("Database is reConnected successfully! Connection name is:"+db->connectionName());
        qDebug()<<"Database is reConnected successfully! Connection name is:"<<db->connectionName();

        return true;
    }
}

bool DBInterfaceAIS::reConnectToDB()
{
    QSqlDatabase::removeDatabase(db->connectionName());
    db->close();
    delete db;

    qDebug()<<"Adding database:"<<dbInfo.connectionName;
    db = new QSqlDatabase(QSqlDatabase::addDatabase(dbInfo.type,dbInfo.connectionName));
    db->setHostName(dbInfo.hostIP);
    db->setDatabaseName(dbInfo.dbName); //Oracle的SID
    db->setUserName(dbInfo.userName);
    db->setPassword(dbInfo.pwd);
    if(!db->open())
    {
        emit sigShowInfo("Can't connect to  database! "+db->lastError().text()+
                         " Connection name is "+db->connectionName());
        qDebug()<<"Can't coonect to db:"+db->connectionName();
        return false;
    }
    else
    {
        emit sigShowInfo("database is connected successfully!"+db->lastError().text()+
                         " Connection name is "+db->connectionName());
        qDebug()<<"database is connected successfully!"+db->lastError().text()+
                  " Connection name is "+db->connectionName();
    }

    delete query1;
    query1=new QSqlQuery(*db);
    query1->setForwardOnly(true);

    QString strSQL;
    //strSQL="select 1 from " TABLE_NAME_FOR_CON_CHECK " limit 1";
    strSQL="show tables";
    if(query1->exec(strSQL))
        return true;
    else
        return false;
}
