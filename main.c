#include <main.h>

static void ThreadLock(pthread_mutex_t *mutex, pthread_cond_t *cond, volatile int *ready) {
    pthread_mutex_lock(mutex);
    while (!*ready) {
        pthread_cond_wait(cond, mutex);
    }
    *ready = 0;
    pthread_mutex_unlock(mutex);
}

static void ThreadUnLock(pthread_mutex_t *mutex, pthread_cond_t *cond, volatile int *ready) {
    pthread_mutex_lock(mutex);
    *ready = 1;
    pthread_cond_signal(cond);
    pthread_mutex_unlock(mutex);
}

static UA_StatusCode WriteServerVariable(char *buffer) {
    variable_write_t *message = (variable_write_t*)buffer;

    char *nodeIdStr = message->name;
    uint8_t *newValue = message->value;

    if (!nodeIdStr || !newValue) {
        return UA_STATUSCODE_BADINVALIDARGUMENT;
    }

    UA_NodeId nodeId = UA_NODEID_STRING_ALLOC(1, nodeIdStr);

    UA_Variant currentValue;
    UA_Variant_init(&currentValue);

    UA_StatusCode retval = UA_Server_readValue(OpcUaServer, nodeId, &currentValue);
    if (retval != UA_STATUSCODE_GOOD || !currentValue.type) {
        UA_NodeId_clear(&nodeId);
        UA_Variant_clear(&currentValue);
        return retval ? retval : UA_STATUSCODE_BADTYPEMISMATCH;
    }

    UA_Variant value;
    UA_Variant_init(&value);

    retval = UA_Variant_setScalarCopy(&value, newValue, currentValue.type);
    if (retval == UA_STATUSCODE_GOOD) {
        retval = UA_Server_writeValue(OpcUaServer, nodeId, value);
    }

    UA_Variant_clear(&value);
    UA_Variant_clear(&currentValue);
    UA_NodeId_clear(&nodeId);

    switch(message->typeKind) {
        case UA_DATATYPEKIND_BOOLEAN:
            if (OpcUaChangeFlagBuffer.UaBoolean != NULL) {
                OpcUaChangeFlagBuffer.UaBoolean[message->index] = 1;
            }
            break;
        case UA_DATATYPEKIND_SBYTE:
            if (OpcUaChangeFlagBuffer.UaSByte != NULL) {
                OpcUaChangeFlagBuffer.UaSByte[message->index] = 1;
            }
            break;
        case UA_DATATYPEKIND_BYTE:
            if (OpcUaChangeFlagBuffer.UaByte != NULL) {
                OpcUaChangeFlagBuffer.UaByte[message->index] = 1;
            }
            break;
        case UA_DATATYPEKIND_INT16:
            if (OpcUaChangeFlagBuffer.UaInt16 != NULL) {
                OpcUaChangeFlagBuffer.UaInt16[message->index] = 1;
            }
            break;
        case UA_DATATYPEKIND_UINT16:
            if (OpcUaChangeFlagBuffer.UaUint16 != NULL) {
                OpcUaChangeFlagBuffer.UaUint16[message->index] = 1;
            }
            break;
        case UA_DATATYPEKIND_INT32:
            if (OpcUaChangeFlagBuffer.UaInt32 != NULL) {
                OpcUaChangeFlagBuffer.UaInt32[message->index] = 1;
            }
            break;
        case UA_DATATYPEKIND_UINT32:
            if (OpcUaChangeFlagBuffer.UaUint32 != NULL) {
                OpcUaChangeFlagBuffer.UaUint32[message->index] = 1;
            }
            break;
        case UA_DATATYPEKIND_INT64:
            if (OpcUaChangeFlagBuffer.UaInt64 != NULL) {
                OpcUaChangeFlagBuffer.UaInt64[message->index] = 1;
            }
            break;
        case UA_DATATYPEKIND_UINT64:
            if (OpcUaChangeFlagBuffer.UaUint64 != NULL) {
                OpcUaChangeFlagBuffer.UaUint64[message->index] = 1;
            }
            break;
        case UA_DATATYPEKIND_FLOAT:
            if (OpcUaChangeFlagBuffer.UaFloat != NULL) {
                OpcUaChangeFlagBuffer.UaFloat[message->index] = 1;
            }
            break;
        case UA_DATATYPEKIND_DOUBLE:
            if (OpcUaChangeFlagBuffer.UaDouble != NULL) {
                OpcUaChangeFlagBuffer.UaDouble[message->index] = 1;
            }
            break;
        case UA_DATATYPEKIND_STRING: {
            if (OpcUaChangeFlagBuffer.UaString != NULL) {
                OpcUaChangeFlagBuffer.UaString[message->index] = 1;
            }
            break;
        }
        default:
            break;
    }

    return retval;
}

static uint8_t CheckUaStringLength(UA_String *str) {
    if (!str || !str->data || str->length == 0) {
        return UA_STRING_ERROR;
    }

    size_t length = strnlen((char*)str->data, str->length);

    if (length >= MAX_STRING_VALUE) {
        str->data[MAX_STRING_VALUE - 1] = '\0';
        str->length = MAX_STRING_VALUE - 1;
        return UA_STRING_OVERFLOW;
    }

    if (length < str->length && str->data[length] != '\0') {
        str->data[length] = '\0';
        str->length = length;
    }

    return UA_STRING_OK;
}

static void GlobalDataChangeCallback(UA_Server *server, UA_UInt32 monitoredItemId, void *monitoredItemContext, const UA_NodeId *nodeId, void *nodeContext, UA_UInt32 attributeId, const UA_DataValue *value) {
    if (!monitoredItemContext || !value || !value->value.data) {
        return;
    }

    variable_context_t *ctx = (variable_context_t *)monitoredItemContext;

    if (mqueue_opcua_to_codesys && registration_active == false) {
        variable_write_t msg = {0};

        msg.message_type = MSG_TYPE_WRITE_VARIABLE;
        msg.index = ctx->index;
        msg.typeKind = ctx->typeKind;

        switch(ctx->typeKind) {
            case UA_DATATYPEKIND_BOOLEAN:
                if (OpcUaChangeFlagBuffer.UaBoolean[ctx->index]) {
                    OpcUaChangeFlagBuffer.UaBoolean[ctx->index] = 0;
                    return;
                }
                memcpy(msg.value, value->value.data, sizeof(UA_Boolean));
                break;
            case UA_DATATYPEKIND_SBYTE:
                if (OpcUaChangeFlagBuffer.UaSByte[ctx->index]) {
                    OpcUaChangeFlagBuffer.UaSByte[ctx->index] = 0;
                    return;
                }
                memcpy(msg.value, value->value.data, sizeof(UA_SByte));
                break;
            case UA_DATATYPEKIND_BYTE:
                if (OpcUaChangeFlagBuffer.UaByte[ctx->index]) {
                    OpcUaChangeFlagBuffer.UaByte[ctx->index] = 0;
                    return;
                }
                memcpy(msg.value, value->value.data, sizeof(UA_Byte));
                break;
            case UA_DATATYPEKIND_INT16:
                if (OpcUaChangeFlagBuffer.UaInt16[ctx->index]) {
                    OpcUaChangeFlagBuffer.UaInt16[ctx->index] = 0;
                    return;
                }
                memcpy(msg.value, value->value.data, sizeof(UA_Int16));
                break;
            case UA_DATATYPEKIND_UINT16:
                if (OpcUaChangeFlagBuffer.UaUint16[ctx->index]) {
                    OpcUaChangeFlagBuffer.UaUint16[ctx->index] = 0;
                    return;
                }
                memcpy(msg.value, value->value.data, sizeof(UA_UInt16));
                break;
            case UA_DATATYPEKIND_INT32:
                if (OpcUaChangeFlagBuffer.UaInt32[ctx->index]) {
                    OpcUaChangeFlagBuffer.UaInt32[ctx->index] = 0;
                    return;
                }
                memcpy(msg.value, value->value.data, sizeof(UA_Int32));
                break;
            case UA_DATATYPEKIND_UINT32:
                if (OpcUaChangeFlagBuffer.UaUint32[ctx->index]) {
                    OpcUaChangeFlagBuffer.UaUint32[ctx->index] = 0;
                    return;
                }
                memcpy(msg.value, value->value.data, sizeof(UA_UInt32));
                break;
            case UA_DATATYPEKIND_INT64:
                if (OpcUaChangeFlagBuffer.UaInt64[ctx->index]) {
                    OpcUaChangeFlagBuffer.UaInt64[ctx->index] = 0;
                    return;
                }
                memcpy(msg.value, value->value.data, sizeof(UA_Int64));
                break;
            case UA_DATATYPEKIND_UINT64:
                if (OpcUaChangeFlagBuffer.UaUint64[ctx->index]) {
                    OpcUaChangeFlagBuffer.UaUint64[ctx->index] = 0;
                    return;
                }
                memcpy(msg.value, value->value.data, sizeof(UA_UInt64));
                break;
            case UA_DATATYPEKIND_FLOAT:
                if (OpcUaChangeFlagBuffer.UaFloat[ctx->index]) {
                    OpcUaChangeFlagBuffer.UaFloat[ctx->index] = 0;
                    return;
                }
                memcpy(msg.value, value->value.data, sizeof(UA_Float));
                break;
            case UA_DATATYPEKIND_DOUBLE:
                if (OpcUaChangeFlagBuffer.UaDouble[ctx->index]) {
                    OpcUaChangeFlagBuffer.UaDouble[ctx->index] = 0;
                    return;
                }
                memcpy(msg.value, value->value.data, sizeof(UA_Double));
                break;
            case UA_DATATYPEKIND_STRING: {
                if (OpcUaChangeFlagBuffer.UaString[ctx->index]) {
                    OpcUaChangeFlagBuffer.UaString[ctx->index] = 0;
                    return;
                }
                UA_String *str = (UA_String*)value->value.data;
                if (str->data && str->length > 0) {
                    size_t copy_len = (str->length < MAX_DATA_SIZE) ? str->length : MAX_DATA_SIZE - 1;
                    memcpy(msg.value, str->data, copy_len);
                    msg.value[copy_len] = '\0';
                }
                break;
            }
            default:
                memcpy(msg.value, value->value.data, sizeof(UA_Boolean));
                break;
        }

        mq_send_msg(mqueue_opcua_to_codesys, &msg, sizeof(msg), 1);
    }
}

static void AllocateMemory(uint8_t *buffer, uint16_t NumberAcceptedParameters) {
    if (buffer != NULL || NumberAcceptedParameters == 0) {
        return;
    }

    size_t TotalSize = NumberAcceptedParameters;

    buffer = calloc(1, TotalSize);
    if (buffer == NULL) {
        return;
    }
}

static void AddVariableToOpcUaServer(char *buffer) {
    variable_registration_t *message = (variable_registration_t*)buffer;

    char *name = message->name;
    char *description = message->description;
    uint8_t typeKind = message->typeKind;
    AccessLevel *pAccessLevel = &message->access_level;
    uint8_t *pValue = message->value;
    double deadbandValue = message->deadbandValue;
    uint16_t NumberAcceptedParameters = message->NumberAcceptedParameters;

    if (NumberAcceptedParameters > 0) {
        switch (typeKind) {
            case UA_DATATYPEKIND_BOOLEAN:
                AllocateMemory(OpcUaChangeFlagBuffer.UaBoolean, NumberAcceptedParameters);
                if (OpcUaChangeFlagBuffer.UaBoolean == NULL) {
                    break;
                }
                OpcUaChangeFlagBuffer.UaBoolean[message->index] = 1;
                break;
            case UA_DATATYPEKIND_SBYTE:
                AllocateMemory(OpcUaChangeFlagBuffer.UaSByte, NumberAcceptedParameters);
                if (OpcUaChangeFlagBuffer.UaSByte == NULL) {
                    break;
                }
                OpcUaChangeFlagBuffer.UaSByte[message->index] = 1;
                break;
            case UA_DATATYPEKIND_BYTE:
                AllocateMemory(OpcUaChangeFlagBuffer.UaByte, NumberAcceptedParameters);
                if (OpcUaChangeFlagBuffer.UaByte == NULL) {
                    break;
                }
                OpcUaChangeFlagBuffer.UaByte[message->index] = 1;
                break;
            case UA_DATATYPEKIND_INT16:
                AllocateMemory(OpcUaChangeFlagBuffer.UaInt16, NumberAcceptedParameters);
                if (OpcUaChangeFlagBuffer.UaInt16 == NULL) {
                    break;
                }
                OpcUaChangeFlagBuffer.UaInt16[message->index] = 1;
                break;
            case UA_DATATYPEKIND_UINT16:
                AllocateMemory(OpcUaChangeFlagBuffer.UaUint16, NumberAcceptedParameters);
                if (OpcUaChangeFlagBuffer.UaUint16 == NULL) {
                    break;
                }
                OpcUaChangeFlagBuffer.UaUint16[message->index] = 1;
                break;
            case UA_DATATYPEKIND_INT32:
                AllocateMemory(OpcUaChangeFlagBuffer.UaInt32, NumberAcceptedParameters);
                if (OpcUaChangeFlagBuffer.UaInt32 == NULL) {
                    break;
                }
                OpcUaChangeFlagBuffer.UaInt32[message->index] = 1;
                break;
            case UA_DATATYPEKIND_UINT32:
                AllocateMemory(OpcUaChangeFlagBuffer.UaUint32, NumberAcceptedParameters);
                if (OpcUaChangeFlagBuffer.UaUint32 == NULL) {
                    break;
                }
                OpcUaChangeFlagBuffer.UaUint32[message->index] = 1;
                break;
            case UA_DATATYPEKIND_INT64:
                AllocateMemory(OpcUaChangeFlagBuffer.UaInt64, NumberAcceptedParameters);
                if (OpcUaChangeFlagBuffer.UaInt64 == NULL) {
                    break;
                }
                OpcUaChangeFlagBuffer.UaInt64[message->index] = 1;
                break;
            case UA_DATATYPEKIND_UINT64:
                AllocateMemory(OpcUaChangeFlagBuffer.UaUint64, NumberAcceptedParameters);
                if (OpcUaChangeFlagBuffer.UaUint64 == NULL) {
                    break;
                }
                OpcUaChangeFlagBuffer.UaUint64[message->index] = 1;
                break;
            case UA_DATATYPEKIND_FLOAT:
                AllocateMemory(OpcUaChangeFlagBuffer.UaFloat, NumberAcceptedParameters);
                if (OpcUaChangeFlagBuffer.UaFloat == NULL) {
                    break;
                }
                OpcUaChangeFlagBuffer.UaFloat[message->index] = 1;
                break;
            case UA_DATATYPEKIND_DOUBLE:
                AllocateMemory(OpcUaChangeFlagBuffer.UaDouble, NumberAcceptedParameters);
                if (OpcUaChangeFlagBuffer.UaDouble == NULL) {
                    break;
                }
                OpcUaChangeFlagBuffer.UaDouble[message->index] = 1;
                break;
            case UA_DATATYPEKIND_STRING:
                AllocateMemory(OpcUaChangeFlagBuffer.UaString, NumberAcceptedParameters);
                if (OpcUaChangeFlagBuffer.UaString == NULL) {
                    break;
                }
                OpcUaChangeFlagBuffer.UaString[message->index] = 1;
                break;
            default:
                break;
        }
    }

#ifdef DEBUG
    printf("[OPC_UA] === AddVariableToOpcUaServer ===\n");
    printf("[OPC_UA] Name: %s\n", name);
    printf("[OPC_UA] Description: %s\n", description);
    printf("[OPC_UA] TypeKind: %d\n", typeKind);
    printf("[OPC_UA] AccessLevel: %d\n", *pAccessLevel);
    printf("[OPC_UA] DeadbandValue: %f\n", deadbandValue);
    printf("[OPC_UA] Value pointer: %p\n", pValue);
    fflush(stdout);

    switch (typeKind) {
        case UA_DATATYPEKIND_BOOLEAN:
            printf("[OPC_UA] Value (boolean): %d\n", *(UA_Boolean*)pValue);
            break;
        case UA_DATATYPEKIND_SBYTE:
            printf("[OPC_UA] Value (sbyte): %d\n", *(UA_SByte*)pValue);
            break;
        case UA_DATATYPEKIND_BYTE:
            printf("[OPC_UA] Value (byte): %u\n", *(UA_Byte*)pValue);
            break;
        case UA_DATATYPEKIND_INT16:
            printf("[OPC_UA] Value (int16): %d\n", *(UA_Int16*)pValue);
            break;
        case UA_DATATYPEKIND_UINT16:
            printf("[OPC_UA] Value (uint16): %u\n", *(UA_UInt16*)pValue);
            break;
        case UA_DATATYPEKIND_INT32:
            printf("[OPC_UA] Value (int32): %d\n", *(UA_Int32*)pValue);
            break;
        case UA_DATATYPEKIND_UINT32:
            printf("[OPC_UA] Value (uint32): %u\n", *(UA_UInt32*)pValue);
            break;
        case UA_DATATYPEKIND_INT64:
            printf("[OPC_UA] Value (int64): %lld\n", *(UA_Int64*)pValue);
            break;
        case UA_DATATYPEKIND_UINT64:
            printf("[OPC_UA] Value (uint64): %llu\n", *(UA_UInt64*)pValue);
            break;
        case UA_DATATYPEKIND_FLOAT:
            printf("[OPC_UA] Value (float): %f\n", *(UA_Float*)pValue);
            break;
        case UA_DATATYPEKIND_DOUBLE:
            printf("[OPC_UA] Value (double): %lf\n", *(UA_Double*)pValue);
            break;
        case UA_DATATYPEKIND_STRING:
            printf("[OPC_UA] Value via pValue (string): %s\n", (char*)pValue);
            printf("[OPC_UA] String length: %zu\n", strlen((char*)pValue));
            break;
        default:
            printf("[OPC_UA] Value (unknown type)\n");
            break;
    }
    printf("[OPC_UA] ===============================\n");
    fflush(stdout);
#endif

    UA_VariableAttributes attr = UA_VariableAttributes_default;

    if (typeKind == UA_DATATYPEKIND_STRING) {
        UA_String srcString = UA_STRING((char *)pValue);

        switch (CheckUaStringLength(&srcString)) {
            case UA_STRING_ERROR:
                break;
            case UA_STRING_OVERFLOW:
                memcpy(pValue, srcString.data, MAX_STRING_VALUE);
                break;
            case UA_STRING_OK:
                break;
        }

        UA_Variant_setScalar(&attr.value, &srcString, &UA_TYPES[UA_TYPES_STRING]);
    } else {
        UA_Variant_setScalar(&attr.value, pValue, &UA_TYPES[typeKind]);
    }

    attr.description = UA_LOCALIZEDTEXT("en-US", description);
    attr.displayName = UA_LOCALIZEDTEXT("en-US", name);
    attr.dataType = UA_TYPES[typeKind].typeId;
    attr.accessLevel = *pAccessLevel;

    UA_NodeId newNodeId = UA_NODEID_STRING(1, name);
    UA_NodeId parentNodeId = UA_NODEID_NUMERIC(0, UA_NS0ID_OBJECTSFOLDER);
    UA_NodeId parentReferenceNodeId = UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES);
    UA_QualifiedName browseName = UA_QUALIFIEDNAME(1, name);

    UA_StatusCode retval = UA_Server_addVariableNode(OpcUaServer, newNodeId, parentNodeId, parentReferenceNodeId, browseName, UA_NODEID_NULL, attr, NULL, NULL);

    if (retval != UA_STATUSCODE_GOOD) {
#ifdef DEBUG
        printf("[OPC_UA] Failed to add variable node: %s\n", UA_StatusCode_name(retval));
        fflush(stdout);
#endif
        return;
    }

    if (*pAccessLevel == READWRITE) {
        variable_context_t *ctx = (variable_context_t *)malloc(sizeof(variable_context_t));
        if (!ctx) {
#ifdef DEBUG
            printf("[OPC_UA] Failed to allocate context for variable: %s\n", name);
            fflush(stdout);
#endif
            return;
        }

        strncpy(ctx->name, name, sizeof(ctx->name)-1);
        ctx->typeKind = typeKind;
        ctx->index = message->index;

        UA_MonitoredItemCreateRequest item;
        UA_MonitoredItemCreateRequest_init(&item);

        item.itemToMonitor.nodeId = newNodeId;
        item.itemToMonitor.attributeId = UA_ATTRIBUTEID_VALUE;
        item.monitoringMode = UA_MONITORINGMODE_REPORTING;
        item.requestedParameters.samplingInterval = 1000.0;
        item.requestedParameters.discardOldest = UA_TRUE;
        item.requestedParameters.queueSize = 10;

        UA_DataChangeFilter filter;
        UA_DataChangeFilter_init(&filter);
        filter.trigger = UA_DATACHANGETRIGGER_STATUSVALUE;

        switch(typeKind) {
            case UA_DATATYPEKIND_SBYTE:
            case UA_DATATYPEKIND_BYTE:
            case UA_DATATYPEKIND_INT16:
            case UA_DATATYPEKIND_UINT16:
            case UA_DATATYPEKIND_INT32:
            case UA_DATATYPEKIND_UINT32:
            case UA_DATATYPEKIND_INT64:
            case UA_DATATYPEKIND_UINT64:
            case UA_DATATYPEKIND_FLOAT:
            case UA_DATATYPEKIND_DOUBLE:
                filter.deadbandType = UA_DEADBANDTYPE_ABSOLUTE;
                filter.deadbandValue = deadbandValue;
                break;

            default:
                filter.deadbandType = UA_DEADBANDTYPE_NONE;
                filter.deadbandValue = 0;
                break;
        }

        UA_ExtensionObject filterExt;
        filterExt.encoding = UA_EXTENSIONOBJECT_DECODED;
        filterExt.content.decoded.type = &UA_TYPES[UA_TYPES_DATACHANGEFILTER];
        filterExt.content.decoded.data = &filter;
        item.requestedParameters.filter = filterExt;

        UA_MonitoredItemCreateResult result = UA_Server_createDataChangeMonitoredItem(OpcUaServer, UA_TIMESTAMPSTORETURN_BOTH, item, ctx, GlobalDataChangeCallback);

        if (result.statusCode != UA_STATUSCODE_GOOD) {
#ifdef DEBUG
            printf("[OPC_UA] Failed to create monitored item for %s: %s\n", name, UA_StatusCode_name(result.statusCode));
            fflush(stdout);
#endif
            free(ctx);
        } else {
#ifdef DEBUG
            printf("[OPC_UA] Monitoring enabled for variable: %s\n", name);
            fflush(stdout);
#endif
        }
        UA_MonitoredItemCreateResult_clear(&result);
    }
}

static void FreeChangeFlagBufferStructs(uint8_t *ChangeFlagBuffer) {
    if (ChangeFlagBuffer != NULL) {
        free(ChangeFlagBuffer);
        ChangeFlagBuffer = NULL;
    }
}

static void FreeChangeFlagBuffer(void) {
    FreeChangeFlagBufferStructs(OpcUaChangeFlagBuffer.UaBoolean);
    FreeChangeFlagBufferStructs(OpcUaChangeFlagBuffer.UaSByte);
    FreeChangeFlagBufferStructs(OpcUaChangeFlagBuffer.UaByte);
    FreeChangeFlagBufferStructs(OpcUaChangeFlagBuffer.UaInt16);
    FreeChangeFlagBufferStructs(OpcUaChangeFlagBuffer.UaUint16);
    FreeChangeFlagBufferStructs(OpcUaChangeFlagBuffer.UaInt32);
    FreeChangeFlagBufferStructs(OpcUaChangeFlagBuffer.UaUint32);
    FreeChangeFlagBufferStructs(OpcUaChangeFlagBuffer.UaInt64);
    FreeChangeFlagBufferStructs(OpcUaChangeFlagBuffer.UaUint64);
    FreeChangeFlagBufferStructs(OpcUaChangeFlagBuffer.UaFloat);
    FreeChangeFlagBufferStructs(OpcUaChangeFlagBuffer.UaDouble);
    FreeChangeFlagBufferStructs(OpcUaChangeFlagBuffer.UaString);
}

static void IncomingPacketManager(uint8_t *buffer, ssize_t length) {
    message_type_t header = *(message_type_t*)buffer;

    switch (header) {
        case MSG_TYPE_START_REGISTRATION:
            if (length == sizeof(message_type_t)) {
                registration_active = true;
#ifdef DEBUG
                printf("[OPC_UA] Registration STARTED\n");
                fflush(stdout);
#endif
            }
            break;

        case MSG_TYPE_VARIABLE_REGISTRATION:
            if (registration_active && length == sizeof(variable_registration_t)) {
                AddVariableToOpcUaServer(buffer);
            } else if (!registration_active) {
#ifdef DEBUG
                printf("[OPC_UA] Ignoring variable - registration not active\n");
                fflush(stdout);
#endif
            }
            break;

        case MSG_TYPE_END_REGISTRATION:
            if (length == sizeof(message_type_t)) {
                registration_active = false;

#ifdef DEBUG
                printf("[OPC_UA] Registration FINISHED\n");
                fflush(stdout);
#endif
                ThreadUnLock(&variable_init_mutex, &variable_init_cond, &variable_init_ready);
            }
            break;

        case MSG_TYPE_WRITE_VARIABLE:
            if (!registration_active) {
                if (length == sizeof(variable_write_t)) {
                    WriteServerVariable(buffer);
                }
            } else {
#ifdef DEBUG
                printf("[OPC_UA] Ignoring write - registration in progress\n");
                fflush(stdout);
#endif
            }
            break;

        case MSG_TYPE_SHUT_DOWN:
#ifdef DEBUG
            printf("[OPC_UA] MSG_TYPE_SHUT_DOWN\n");
            fflush(stdout);
#endif

            FreeChangeFlagBuffer();

            ThreadUnLock(&codesys_to_opcua_shutdown_mutex, &codesys_to_opcua_shutdown_cond, &codesys_to_opcua_shutdown);
            ThreadUnLock(&opcua_to_codesys_shutdown_mutex, &opcua_to_codesys_shutdown_cond, &opcua_to_codesys_shutdown);
            opcua_server_pthread_running = false;
            break;

        default:
#ifdef DEBUG
            printf("[OPC_UA] Unknown message type: %d\n", header);
            fflush(stdout);
#endif
            break;
    }
}

static void CodesysToOpcUaMessageHandler(union sigval sv) {
    mqd_t mq = *((mqd_t*)sv.sival_ptr);
    uint8_t buffer[MAX_MSG_SIZE];
    ssize_t received;

    do {
        received = mq_receive_msg(mq, buffer, sizeof(buffer), NULL);
        if (received > 0) {
            IncomingPacketManager(&buffer, received);
        }
    } while (received > 0);

    struct sigevent notification;
    notification.sigev_notify = SIGEV_THREAD;
    notification.sigev_notify_function = CodesysToOpcUaMessageHandler;
    notification.sigev_notify_attributes = NULL;
    notification.sigev_value.sival_ptr = sv.sival_ptr;

    mq_set_notification(mq, &notification);
}

static void *CodesysToOpcUaPthread(void *arg) {
    mqueue_codesys_to_opcua = mq_init(QUEUE_NAME_CODESYS_TO_OPCUA, 5, MAX_MSG_SIZE, O_CREAT | O_RDONLY | O_NONBLOCK);
    if (mqueue_codesys_to_opcua == -1) {
        perror("mqueue_codesys_to_opcua failed");
        exit(EXIT_FAILURE);
    }

    struct sigevent notification;
    notification.sigev_notify = SIGEV_THREAD;
    notification.sigev_notify_function = CodesysToOpcUaMessageHandler;
    notification.sigev_notify_attributes = NULL;
    notification.sigev_value.sival_ptr = &mqueue_codesys_to_opcua;

    if (mq_set_notification(mqueue_codesys_to_opcua, &notification) != 0) {
        perror("mq_set_notification failed");
        exit(EXIT_FAILURE);
    }

    ThreadUnLock(&codesys_to_opcua_ready_mutex, &codesys_to_opcua_ready_cond, &codesys_to_opcua_ready);

    ThreadLock(&codesys_to_opcua_shutdown_mutex, &codesys_to_opcua_shutdown_cond, &codesys_to_opcua_shutdown);

    mq_set_notification(mqueue_codesys_to_opcua, NULL);
    mq_close_queue(mqueue_codesys_to_opcua);
    mq_unlink_queue(QUEUE_NAME_CODESYS_TO_OPCUA);
    mqueue_codesys_to_opcua = -1;

#ifdef DEBUG
    printf("[OPC_UA] CodesysToOpcUaPthread shutdown.\n");
    fflush(stdout);
#endif

    return NULL;
}

static void *OpcUaToCodesysPthread(void *arg) {
    mqueue_opcua_to_codesys = mq_init(QUEUE_NAME_OPCUA_TO_CODESYS, 5, MAX_MSG_SIZE, O_CREAT | O_WRONLY | O_NONBLOCK);
    if (mqueue_opcua_to_codesys == -1) {
        perror("mqueue_opcua_to_codesys failed");
        exit(EXIT_FAILURE);
    }

    ThreadUnLock(&opcua_to_codesys_ready_mutex, &opcua_to_codesys_ready_cond, &opcua_to_codesys_ready);

    ThreadLock(&opcua_to_codesys_shutdown_mutex, &opcua_to_codesys_shutdown_cond, &opcua_to_codesys_shutdown);

    mq_close_queue(mqueue_opcua_to_codesys);
    mq_unlink_queue(QUEUE_NAME_OPCUA_TO_CODESYS);
    mqueue_opcua_to_codesys = -1;

#ifdef DEBUG
    printf("[OPC_UA] OpcUaToCodesysPthread shutdown.\n");
    fflush(stdout);
#endif

    return NULL;
}

static void *OpcUaServerPthread(void *arg) {
    OpcUaServer = UA_Server_new();

    if (!OpcUaServer) {
        perror("[OPC_UA] OpcUaServerPthread channel create()");
        exit(EXIT_FAILURE);
    }

    UA_ServerConfig_setDefault(UA_Server_getConfig(OpcUaServer));
    UA_ServerConfig* config = UA_Server_getConfig(OpcUaServer);
    config->verifyRequestTimestamp = UA_RULEHANDLING_ACCEPT;

    config->maxSessions = 20;
    config->maxSubscriptions = 200;
    config->maxPublishReqPerSession = 100;
    config->queueSizeLimits.max = 10000;
    config->maxSessionTimeout = 30000;
    config->publishingIntervalLimits.min = 100;
    config->samplingIntervalLimits.min = 50;

#ifdef UA_ENABLE_WEBSOCKET_SERVER
    UA_ServerConfig_addNetworkLayerWS(UA_Server_getConfig(OpcUaServer), 7681, 0, 0, NULL, NULL);
#endif

    ThreadUnLock(&opcua_server_ready_mutex, &opcua_server_ready_cond, &opcua_server_ready);

#ifdef DEBUG
    config->logging->context = UA_LOGLEVEL_INFO;
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "New OPC UA Server %i\n", OpcUaServer);
#else
    config->logging->context = UA_LOGLEVEL_FATAL;
#endif

    ThreadLock(&variable_init_mutex, &variable_init_cond, &variable_init_ready);

    opcua_server_pthread_running = true;

    UA_Server_run(OpcUaServer, &opcua_server_pthread_running);

    UA_Server_run_shutdown(OpcUaServer);

    UA_Server_delete(OpcUaServer);

#ifdef DEBUG
    printf("[OPC_UA] OpcUaServerPthread shutdown. \n");
    fflush(stdout);
#endif

    return NULL;
}

static int InitializeSyncPrimitives(void) {
    int result = 0;

    if (pthread_mutex_init(&variable_init_mutex, NULL) != 0) {
        perror("Failed to initialize variable_init_mutex");
        result = -1;
    }
    if (pthread_mutex_init(&opcua_server_ready_mutex, NULL) != 0) {
        perror("Failed to initialize opcua_server_ready_mutex");
        result = -1;
    }
    if (pthread_mutex_init(&codesys_to_opcua_ready_mutex, NULL) != 0) {
        perror("Failed to initialize codesys_to_opcua_ready_mutex");
        result = -1;
    }
    if (pthread_mutex_init(&codesys_to_opcua_shutdown_mutex, NULL) != 0) {
        perror("Failed to initialize codesys_to_opcua_shutdown_mutex");
        result = -1;
    }
    if (pthread_mutex_init(&opcua_to_codesys_ready_mutex, NULL) != 0) {
        perror("Failed to initialize opcua_to_codesys_ready_mutex");
        result = -1;
    }
    if (pthread_mutex_init(&opcua_to_codesys_shutdown_mutex, NULL) != 0) {
        perror("Failed to initialize opcua_to_codesys_shutdown_mutex");
        result = -1;
    }

    if (pthread_cond_init(&variable_init_cond, NULL) != 0) {
        perror("Failed to initialize variable_init_cond");
        result = -1;
    }
    if (pthread_cond_init(&opcua_server_ready_cond, NULL) != 0) {
        perror("Failed to initialize opcua_server_ready_cond");
        result = -1;
    }
    if (pthread_cond_init(&codesys_to_opcua_ready_cond, NULL) != 0) {
        perror("Failed to initialize codesys_to_opcua_ready_cond");
        result = -1;
    }
    if (pthread_cond_init(&codesys_to_opcua_shutdown_cond, NULL) != 0) {
        perror("Failed to initialize codesys_to_opcua_shutdown_cond");
        result = -1;
    }
    if (pthread_cond_init(&opcua_to_codesys_ready_cond, NULL) != 0) {
        perror("Failed to initialize opcua_to_codesys_ready_cond");
        result = -1;
    }
    if (pthread_cond_init(&opcua_to_codesys_shutdown_cond, NULL) != 0) {
        perror("Failed to initialize opcua_to_codesys_shutdown_cond");
        result = -1;
    }

    variable_init_ready = 0;
    opcua_server_ready = 0;
    codesys_to_opcua_ready = 0;
    codesys_to_opcua_shutdown = 0;
    opcua_to_codesys_ready = 0;
    opcua_to_codesys_shutdown = 0;

#ifdef DEBUG
    if (result == 0) {
        printf("[OPC_UA] All synchronization primitives initialized successfully\n");
    } else {
        printf("[OPC_UA] Some synchronization primitives failed to initialize\n");
    }
    fflush(stdout);
#endif

    return result;
}

int main(int argc, char* argv[]) {
    memset(&OpcUaChangeFlagBuffer, 0, sizeof(OpcUaChangeFlagBuffer));

    InitializeSyncPrimitives();

    /*******************************************************************/

#ifdef DEBUG
    printf("[OPC_UA] starting CODESYS_TO_OPCUA_THREAD\n");
    fflush(stdout);
#endif

    pthread_t CODESYS_TO_OPCUA_THREAD;
    if (pthread_create(&CODESYS_TO_OPCUA_THREAD, NULL, CodesysToOpcUaPthread, NULL)) {
        perror("[OPC_UA] CodesysToOpcUaPthread create ");
        return EXIT_FAILURE;
    }
    ThreadLock(&codesys_to_opcua_ready_mutex, &codesys_to_opcua_ready_cond, &codesys_to_opcua_ready);

#ifdef DEBUG
    printf("[OPC_UA] CODESYS_TO_OPCUA_THREAD is ready\n");
    fflush(stdout);
#endif

    /*******************************************************************/

#ifdef DEBUG
    printf("[OPC_UA] starting OPCUA_TO_CODESYS_THREAD\n");
    fflush(stdout);
#endif

    pthread_t OPCUA_TO_CODESYS_THREAD;
    if (pthread_create(&OPCUA_TO_CODESYS_THREAD, NULL, OpcUaToCodesysPthread, NULL)) {
        perror("[OPC_UA] OpcUaToCodesysPthread create ");
        return EXIT_FAILURE;
    }
    ThreadLock(&opcua_to_codesys_ready_mutex, &opcua_to_codesys_ready_cond, &opcua_to_codesys_ready);

#ifdef DEBUG
    printf("[OPC_UA] OPCUA_TO_CODESYS_THREAD is ready\n");
    fflush(stdout);
#endif

    /*******************************************************************/

#ifdef DEBUG
    printf("[OPC_UA] starting OPCUA_SERVER_THREAD\n");
    fflush(stdout);
#endif

    pthread_t OPCUA_SERVER_THREAD;
    if (pthread_create(&OPCUA_SERVER_THREAD, NULL, OpcUaServerPthread, NULL)) {
        perror("[OPC_UA] OpcUaServerPthread create ");
        return EXIT_FAILURE;
    }
    ThreadLock(&opcua_server_ready_mutex, &opcua_server_ready_cond, &opcua_server_ready);

#ifdef DEBUG
    printf("[OPC_UA] OPCUA_SERVER_THREAD is ready\n");
    fflush(stdout);
#endif

    /*******************************************************************/

    pthread_join(CODESYS_TO_OPCUA_THREAD, NULL);
    pthread_join(OPCUA_TO_CODESYS_THREAD, NULL);
    pthread_join(OPCUA_SERVER_THREAD, NULL);

    return EXIT_SUCCESS;
}
