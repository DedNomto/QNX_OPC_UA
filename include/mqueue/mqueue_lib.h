#ifndef MQUEUE_LIB_H
#define MQUEUE_LIB_H

#include <fcntl.h>
#include <sys/types.h>
#include <signal.h>
#include <errno.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declarations */
struct mq_attr;
struct sigevent;
struct timespec;

/**
 * @brief Инициализирует очередь сообщений
 *
 * @param name Имя очереди (должно начинаться с /)
 * @param maxmsg Максимальное количество сообщений
 * @param msgsize Максимальный размер сообщения
 * @param flags Флаги (O_RDONLY, O_WRONLY, O_RDWR, O_CREAT, O_EXCL, O_NONBLOCK)
 * @return int Дескриптор очереди или -1 при ошибке
 */
int mq_init(const char *name, long maxmsg, long msgsize, int flags);

/**
 * @brief Отправляет сообщение в очередь
 *
 * @param mqdes Дескриптор очереди
 * @param msg Указатель на данные сообщения
 * @param len Длина сообщения
 * @param prio Приоритет сообщения
 * @return int 0 при успехе, -1 при ошибке
 */
int mq_send_msg(int mqdes, const void *msg, size_t len, unsigned prio);

/**
 * @brief Получает сообщение из очереди
 *
 * @param mqdes Дескриптор очереди
 * @param msg Буфер для сообщения
 * @param len Размер буфера
 * @param prio Указатель для получения приоритета (может быть NULL)
 * @return ssize_t Длина полученного сообщения или -1 при ошибке
 */
ssize_t mq_receive_msg(int mqdes, void *msg, size_t len, unsigned *prio);

/**
 * @brief Отправляет сообщение с таймаутом
 *
 * @param mqdes Дескриптор очереди
 * @param msg Указатель на данные сообщения
 * @param len Длина сообщения
 * @param prio Приоритет сообщения
 * @param timeout Время ожидания
 * @return int 0 при успехе, -1 при ошибке
 */
int mq_send_timed(int mqdes, const void *msg, size_t len, unsigned prio,
                  const struct timespec *timeout);

/**
 * @brief Получает сообщение с таймаутом
 *
 * @param mqdes Дескриптор очереди
 * @param msg Буфер для сообщения
 * @param len Размер буфера
 * @param prio Указатель для получения приоритета
 * @param timeout Время ожидания
 * @return ssize_t Длина полученного сообщения или -1 при ошибке
 */
ssize_t mq_receive_timed(int mqdes, void *msg, size_t len, unsigned *prio,
                        const struct timespec *timeout);

/**
 * @brief Устанавливает атрибуты очереди
 *
 * @param mqdes Дескриптор очереди
 * @param flags Флаги (0 или O_NONBLOCK)
 * @return int 0 при успехе, -1 при ошибке
 */
int mq_set_attributes(int mqdes, long flags);

/**
 * @brief Получает атрибуты очереди
 *
 * @param mqdes Дескриптор очереди
 * @param maxmsg Указатель для максимального количества сообщений
 * @param msgsize Указатель для максимального размера сообщения
 * @param flags Указатель для флагов
 * @param curmsgs Указатель для текущего количества сообщений
 * @return int 0 при успехе, -1 при ошибке
 */
int mq_get_attributes(int mqdes, long *maxmsg, long *msgsize, long *flags,
                     long *curmsgs);

/**
 * @brief Устанавливает уведомление о поступлении сообщения
 *
 * @param mqdes Дескриптор очереди
 * @param notification Структура уведомления
 * @return int 0 при успехе, -1 при ошибке
 */
int mq_set_notification(int mqdes, const struct sigevent *notification);

/**
 * @brief Закрывает очередь сообщений
 *
 * @param mqdes Дескриптор очереди
 * @return int 0 при успехе, -1 при ошибке
 */
int mq_close_queue(int mqdes);

/**
 * @brief Удаляет очередь сообщений из системы
 *
 * @param name Имя очереди
 * @return int 0 при успехе, -1 при ошибке
 */
int mq_unlink_queue(const char *name);

/**
 * @brief Проверяет, существует ли очередь
 *
 * @param name Имя очереди
 * @return int 1 если существует, 0 если нет, -1 при ошибке
 */
int mq_exists(const char *name);

#ifdef __cplusplus
}
#endif

#endif /* MQUEUE_LIB_H */
