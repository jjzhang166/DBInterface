QT += core sql
QT -= gui

CONFIG += c++11

TARGET = DBInterface
CONFIG += console
CONFIG -= app_bundle

TEMPLATE = app

SOURCES += main.cpp \
    dbinterfaceais.cpp

HEADERS += \
    dbinterfaceais.h
