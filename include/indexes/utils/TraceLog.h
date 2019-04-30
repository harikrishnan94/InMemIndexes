/*------------------------------------------------------------------------
  Copyright (c) 2016 Jeff Preshing
  Distributed under the Simplified BSD License.
  Original location: https://github.com/preshing/turf
  This software is distributed WITHOUT ANY WARRANTY; without even the
  implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the LICENSE file for more information.
------------------------------------------------------------------------*/

#pragma once

#include "ThreadLocal.h"

#include <atomic>
#include <memory>
#include <mutex>

namespace indexes::utils {
//---------------------------------------------------------
// Logs TRACE events to memory.
// Iterator should only be used after logging is complete.
// Useful for post-mortem debugging and for validating tests.
//---------------------------------------------------------
class TraceLog {
public:
  struct Event {
    int tid;
    const char *fmt;
    uintptr_t param1;
    uintptr_t param2;

    Event() : fmt(NULL), param1(0), param2(0) {}
  };

private:
  static constexpr unsigned MaxNumPages = 4;
  static constexpr unsigned EventsPerPage = 16384;

  struct Page {
    Page *next;
    // This can exceed EVENTS_PER_PAGE, but it's harmless. Just means page is
    // full.
    std::atomic<int> index;
    Event events[EventsPerPage];

    Page() : next(NULL), index(0) {}
  };

  // Store events in a linked list of pages.
  // Mutex is only locked when it's time to allocate a new page.
  std::mutex m_mutex;
  Page *m_head;
  std::atomic<Page *> m_tail;
  unsigned m_numPages; // Protected by m_mutex

  Event *allocateEventFromNewPage();

public:
  TraceLog();
  ~TraceLog();

  void log(const char *fmt, uintptr_t param1, uintptr_t param2) {
    std::atomic_signal_fence(std::memory_order_seq_cst); // Compiler barrier

    Page *page = m_tail.load(std::memory_order_consume);
    Event *evt;
    int index = page->index.fetch_add(1, std::memory_order_relaxed);

    if (static_cast<unsigned>(index) < EventsPerPage)
      evt = &page->events[index];
    else
      evt = allocateEventFromNewPage(); // Double-checked locking is performed
                                        // inside here.

    evt->tid = ThreadLocal::ThreadID();
    evt->fmt = fmt;
    evt->param1 = param1;
    evt->param2 = param2;

    std::atomic_signal_fence(std::memory_order_seq_cst); // Compiler barrier
  }

  // Iterators are meant to be used only after all logging is complete.
  friend class Iterator;
  class Iterator {
  private:
    Page *m_page;
    int m_index;

  public:
    Iterator(Page *p, int i) : m_page(p), m_index(i) {}

    Iterator &operator++() {
      m_index++;

      if (static_cast<unsigned>(m_index) >= EventsPerPage) {
        Page *next = m_page->next;
        if (next) {
          m_page = next;
          m_index = 0;
        } else {
          m_index = m_page->index.load(std::memory_order_relaxed);
        }
      }

      return *this;
    }

    bool operator!=(const Iterator &other) const {
      return (m_page != other.m_page) || (m_index != other.m_index);
    }

    const Event &operator*() const { return m_page->events[m_index]; }
  };

  Iterator begin() { return Iterator(m_head, 0); }

  Iterator end() {
    Page *tail = m_tail.load(std::memory_order_relaxed);

    return Iterator(tail, tail->index.load(std::memory_order_relaxed));
  }

  void dumpStats();
  void dumpEntireLog(const char *path = nullptr, unsigned startPage = 0);

  static TraceLog Instance;
};

} // namespace indexes::utils

#define TRACELOG(fmt, param1, param2)                                          \
  indexes::utils::TraceLog::Instance.log(fmt,                                  \
                                         reinterpret_cast<uintptr_t>(param1),  \
                                         reinterpret_cast<uintptr_t>(param2))
#define TRACELOG1(fmt, param1) TRACELOG(fmt, param1, 0UL)
