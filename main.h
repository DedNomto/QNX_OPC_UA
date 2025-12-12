#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <stdint.h>
#include <sys/neutrino.h>
#include <confname.h>
#include <sys/stat.h>
#include <string.h>

#include <mqueue.h>
#include <mqueue_lib.h>
#include <open62541/server.h>
#include <open62541/plugin/log_stdout.h>
#include <open62541/server_config_default.h>
#include <signal.h>

/* MQUEUE */

#define QUEUE_NAME_CODESYS_TO_OPCUA "/codesys_to_opcua"
#define QUEUE_NAME_OPCUA_TO_CODESYS "/opcua_to_codesys"
#define MAX_MSG_SIZE 1024

static int mqueue_codesys_to_opcua = -1;
static int mqueue_opcua_to_codesys = -1;

/* OPC UA */

UA_Server *OpcUaServer = NULL;

volatile UA_Boolean opcua_server_pthread_running = true;

    /*******************************************************************/

pthread_mutex_t variable_init_mutex;
pthread_cond_t variable_init_cond;
volatile int variable_init_ready = 0;

    /*******************************************************************/

pthread_mutex_t opcua_server_ready_mutex;
pthread_cond_t opcua_server_ready_cond;
volatile int opcua_server_ready = 0;

    /*******************************************************************/

pthread_mutex_t codesys_to_opcua_ready_mutex;
pthread_cond_t codesys_to_opcua_ready_cond;
volatile int codesys_to_opcua_ready = 0;

pthread_mutex_t codesys_to_opcua_shutdown_mutex;
pthread_cond_t codesys_to_opcua_shutdown_cond;
volatile int codesys_to_opcua_shutdown = 0;

    /*******************************************************************/

pthread_mutex_t opcua_to_codesys_ready_mutex;
pthread_cond_t opcua_to_codesys_ready_cond;
volatile int opcua_to_codesys_ready = 0;

pthread_mutex_t opcua_to_codesys_shutdown_mutex;
pthread_cond_t opcua_to_codesys_shutdown_cond;
volatile int opcua_to_codesys_shutdown = 0;

    /*******************************************************************/

#define MAX_NAME_LENGTH             32
#define MAX_DATA_SIZE               MAX_NAME_LENGTH
#define MAX_DESCRIPTION_LENGTH      64
#define MAX_STRING_VALUE            32

#define UA_STRING_ERROR             0
#define UA_STRING_OVERFLOW          1
#define UA_STRING_OK                2

typedef uint8_t AccessLevel;
enum {
    READ = 1,
    WRITE = 2, // !!!
    READWRITE = 3
};

typedef enum {
    MSG_TYPE_START_REGISTRATION = 0xFA,
    MSG_TYPE_VARIABLE_REGISTRATION = 0xFB,
    MSG_TYPE_END_REGISTRATION = 0xFC,
    MSG_TYPE_WRITE_VARIABLE = 0xFD,
    MSG_TYPE_SHUT_DOWN = 0xFE,
} message_type_t;

static bool registration_active = false;
typedef void*   RTS_HANDLE;

typedef struct {
    uint8_t *UaBoolean;
    uint8_t *UaSByte;
    uint8_t *UaByte;
    uint8_t *UaInt16;
    uint8_t *UaUint16;
    uint8_t *UaInt32;
    uint8_t *UaUint32;
    uint8_t *UaInt64;
    uint8_t *UaUint64;
    uint8_t *UaFloat;
    uint8_t *UaDouble;
    uint8_t *UaString;
} change_flag_buffer;

change_flag_buffer OpcUaChangeFlagBuffer;

typedef struct {
    message_type_t message_type;
    UA_DataTypeKind typeKind;
    char name[MAX_NAME_LENGTH];
    char description[MAX_DESCRIPTION_LENGTH];
    AccessLevel access_level;
    uint8_t value[MAX_DATA_SIZE];
    double deadbandValue;
    uint16_t index;
    uint16_t NumberAcceptedParameters;
} variable_registration_t;

typedef struct {
    message_type_t message_type;
    char name[MAX_NAME_LENGTH];
    uint8_t value[MAX_DATA_SIZE];
    uint16_t index;
    UA_DataTypeKind typeKind;
} variable_write_t;

typedef struct {
    uint8_t typeKind;
    char name[MAX_NAME_LENGTH];
    uint16_t index;
} variable_context_t;
