// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdexcept>
#include <string.h>
#include <iomanip>

#include <sys/errno.h>

#include "util/logger.h"
#include "posix_sys.h"

using namespace std;

namespace cascadb {
    
static exception pthread_call_exception(const char *label, int result) {
    string what = "pthread call ";
    what += label;
    what += ", reason: ";
    what += strerror(result);
    return runtime_error(what);
}

static void pthread_call(const char* label, int result) {
  if (result != 0) {
    throw pthread_call_exception(label, result);
  }
}

Thread::Thread(func f) : f_(f), alive_(false)
{
}

Thread::~Thread()
{
    if(alive_) join();
}

void Thread::start(void* arg)
{
    pthread_call("start thread", pthread_create(&thr_, NULL, f_, arg));
    alive_ = true;
}

void Thread::join()
{
    if(alive_) {
        pthread_call("join thread", pthread_join(thr_, NULL));
        alive_ = false;
    }
}

Mutex::Mutex() {
    locked_ = false;
    pthread_call("init mutex", pthread_mutex_init(&mu_, NULL));
}

Mutex::~Mutex() {
    pthread_call("destroy mutex", pthread_mutex_destroy(&mu_));
}

void Mutex::lock() {
    int res = pthread_mutex_lock(&mu_);
    if (res == 0) {
        locked_ = true;
    } else {
        throw pthread_call_exception("lock", res);
    }
}

bool Mutex::lock_try() {
    int res = pthread_mutex_trylock(&mu_);
    if (res == 0) {
        locked_ = true;
        return true;
    } else if (res == EBUSY || res == EAGAIN) {
        return false;
    } else {
        throw pthread_call_exception("trylock", res);
    }
}

bool Mutex::lock_try(unsigned int millisec) {
    struct timespec timeout;
    Time t = now();
    
    timeout.tv_nsec = t.tv_usec*1000 + millisec*1000000;
    timeout.tv_sec = t.tv_sec + timeout.tv_nsec/1000000000;
    timeout.tv_nsec = timeout.tv_nsec%1000000000;
    
    int res = pthread_mutex_timedlock(&mu_, &timeout);
    if (res == 0) {
        locked_ = true;
        return true;
    } else if (res == ETIMEDOUT) {
        return false;
    } else {
        throw pthread_call_exception("timedlock", res);
    }
}

void Mutex::unlock() {
    int res = pthread_mutex_unlock(&mu_);
    if (res == 0) {
        locked_ = false;
    } else {
        throw pthread_call_exception("unlock", res);
    }
}

RWLock::RWLock()
{
    pthread_call("init rwlock", pthread_rwlock_init(&rwlock_, NULL));
}

RWLock::~RWLock()
{
    pthread_call("destroy rwlock", pthread_rwlock_destroy(&rwlock_));
}

void RWLock::read_lock()
{
    pthread_call("rdlock", pthread_rwlock_rdlock(&rwlock_));
}

bool RWLock::try_read_lock()
{
    int res = pthread_rwlock_tryrdlock(&rwlock_);
    if (res == 0) {
        return true;
    } else if (res == EBUSY) {
        return false;
    } else {
        throw pthread_call_exception("tryrdlock", res);
    }
}
    
bool RWLock::try_read_lock(unsigned int millisec)
{
    struct timespec timeout;
    Time t = now();
    
    timeout.tv_nsec = t.tv_usec*1000 + millisec*1000000;
    timeout.tv_sec = t.tv_sec + timeout.tv_nsec/1000000000;
    timeout.tv_nsec = timeout.tv_nsec%1000000000;
    
    int res = pthread_rwlock_timedrdlock(&rwlock_, &timeout);
    if (res == 0) {
        return true;
    } else if (res == ETIMEDOUT) {
        return false;
    } else {
        throw pthread_call_exception("timedrdlock", res);
    }
}
    
void RWLock::write_lock()
{
    pthread_call("wrlock", pthread_rwlock_wrlock(&rwlock_));
}

bool RWLock::try_write_lock()
{
    int res = pthread_rwlock_trywrlock(&rwlock_);
    if (res == 0) {
        return true;
    } else if (res == EBUSY) {
        return false;
    } else {
        throw pthread_call_exception("trywrlock", res);
    }
}

bool RWLock::try_write_lock(unsigned int millisec)
{
    struct timespec timeout;
    Time t = now();
    
    timeout.tv_nsec = t.tv_usec*1000 + millisec*1000000;
    timeout.tv_sec = t.tv_sec + timeout.tv_nsec/1000000000;
    timeout.tv_nsec = timeout.tv_nsec%1000000000;
    
    int res = pthread_rwlock_timedwrlock(&rwlock_, &timeout);
    if (res == 0) {
        return true;
    } else if (res == ETIMEDOUT) {
        return false;
    } else {
        throw pthread_call_exception("timedwrlock", res);
    }
}

void RWLock::unlock()
{
    pthread_call("rwlock unlock", pthread_rwlock_unlock(&rwlock_));
}

CondVar::CondVar(Mutex* mu) : mu_(mu) {
    pthread_call("init cv", pthread_cond_init(&cv_, NULL));
}

CondVar::~CondVar() {
    pthread_call("destroy cv", pthread_cond_destroy(&cv_));
}

void CondVar::wait() {
    pthread_call("wait", pthread_cond_wait(&cv_, &mu_->mu_));
}

bool CondVar::wait(unsigned int millisec) {
    struct timespec timeout;
    Time t = now();
    
    timeout.tv_nsec = t.tv_usec*1000 + millisec*1000000;
    timeout.tv_sec = t.tv_sec + timeout.tv_nsec/1000000000;
    timeout.tv_nsec = timeout.tv_nsec%1000000000;
    
    int res = pthread_cond_timedwait(&cv_, &mu_->mu_, &timeout);
    if (res == 0) {
        return true;
    } else if (res == ETIMEDOUT) {
        return false;
    } else {
        throw pthread_call_exception("cv timedwait", res);
    }
}

void CondVar::notify() {
    pthread_call("notify", pthread_cond_signal(&cv_));
}

void CondVar::notify_all() {
    pthread_call("notify all", pthread_cond_broadcast(&cv_));
}

/**************************************
 *   cron
 *************************************/

Cron::~Cron()
{
    if (alive_)
        shutdown();

    if (thread_) {
        delete thread_;
    }
}

static void cron_gettime(struct timespec *a)
{
    struct timeval tv;
    gettimeofday(&tv, 0);
    a->tv_sec = tv.tv_sec;
    a->tv_nsec = tv.tv_usec * 1000LL;
}

static void *do_cron(void *arg)
{
    Cron *cron = (Cron*) arg;

    pthread_mutex_lock(&cron->PMTX);
    while (1) {
        if (!cron->alive()) {
            pthread_mutex_unlock(&cron->PMTX);
            return 0;
        }

        if (cron->period_ms() == 0) {
            pthread_cond_wait(&cron->PCOND, &cron->PMTX);
        }
        else {
            unsigned int p_ms = cron->period_ms();
            struct timespec timeout = cron->last_call_time_;

            timeout.tv_nsec += (p_ms * 1000000);
            timeout.tv_sec += (timeout.tv_nsec / 1000000000);
            timeout.tv_nsec = (timeout.tv_nsec % 1000000000);

            int r = pthread_cond_timedwait(&cron->PCOND, &cron->PMTX, &timeout);
            if (r == 0 || r == ETIMEDOUT) {
                // do job
                if (cron->alive()) {
                    pthread_mutex_unlock(&cron->PMTX);
                    cron->f()(cron->arg());
                    cron_gettime(&cron->last_call_time_);
                    pthread_mutex_lock(&cron->PMTX);
                }
            } else {
                LOG_ERROR(" do cron error, " << strerror(r));
            }
        }

        if (!cron->alive()) {
            pthread_mutex_unlock(&cron->PMTX);
            return 0;
        }
    }

    return NULL;
}

bool Cron::init(func f, void *arg, uint32_t period_ms)
{
    init_ = true;

    pthread_cond_init(&PCOND, NULL);
    pthread_mutex_init(&PMTX, NULL);

    f_ = f;
    arg_ = arg;
    period_ms_ = period_ms;
    cron_gettime(&last_call_time_);

    thread_ = new Thread(do_cron);
    if (thread_)
        return true;

    return false;
}

void Cron::start()
{
    if (alive_ || !init_)
        return;

    alive_ = true;
    if (thread_) {
        thread_->start(this);
    }
}

void Cron::shutdown()
{
    if (!alive_) return;

    pthread_mutex_lock(&PMTX);
    alive_ = false;
    pthread_cond_signal(&PCOND);
    pthread_mutex_unlock(&PMTX);

    thread_->join();
    pthread_cond_destroy(&PCOND);
    pthread_mutex_destroy(&PMTX);
}

bool Cron::change_period(unsigned int period_ms)
{
    pthread_mutex_lock(&PMTX);
    period_ms_ = period_ms;
    // to wake up
    pthread_cond_signal(&PCOND);
    pthread_mutex_unlock(&PMTX);

    return true;
}

/**************************************
 *   cron end
 *************************************/

bool operator<(const Time& t1, const Time& t2)
{
    int c = t1.tv_sec - t2.tv_sec;
    if (c < 0) {
        return true;
    } else if (c > 0) {
        return false;
    } else {
        return t1.tv_usec < t2.tv_usec;
    }
    
}

Time now()
{
    Time t;
    ::gettimeofday(&t, NULL);
    return t;
}

extern std::ostream& operator<<(std::ostream& os, const Time& t)
{
    string time = ctime(&t.tv_sec);
    time.erase(time.find('\n', 0), 1);
    os << time << ", " << setw(6) << t.tv_usec;
    return os;
}

extern uint64_t now_micros()
{
    Time t = now();
    return t.tv_sec * 1000000 + t.tv_usec;
}

void sleep(Second sec)
{
    ::sleep(sec);
}

void usleep(USecond usec)
{
    ::usleep(usec);
}

USecond interval_us(Time t1, Time t2)
{
    return (t2.tv_sec - t1.tv_sec) * 1000000 +
        (t2.tv_usec - t1.tv_usec);
}

}
