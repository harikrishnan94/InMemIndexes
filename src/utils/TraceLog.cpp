/*------------------------------------------------------------------------
  Turf: Configurable C++ platform adapter
  Copyright (c) 2016 Jeff Preshing
  Distributed under the Simplified BSD License.
  Original location: https://github.com/preshing/turf
  This software is distributed WITHOUT ANY WARRANTY; without even the
  implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the LICENSE file for more information.
------------------------------------------------------------------------*/

#include "indexes/utils/TraceLog.h"

#include <algorithm>
#include <memory>
#include <stdio.h>

namespace btree::utils
{
TraceLog TraceLog::Instance;

TraceLog::TraceLog() : m_head(new Page), m_tail(m_head), m_numPages(1)
{}

TraceLog::~TraceLog()
{
	// Pages are not cleaned up
}

TraceLog::Event *
TraceLog::allocateEventFromNewPage()
{
	std::lock_guard<std::mutex> lock(m_mutex);

	// Double-checked locking:
	// Check again whether the current page is full. Another thread may have called
	// allocateEventFromNewPage and created a new page by the time we get take the lock.
	Page *oldTail = m_tail.load(std::memory_order_relaxed);

	if (static_cast<unsigned>(oldTail->index.load(std::memory_order_relaxed)) < EventsPerPage)
	{
		int index = oldTail->index.fetch_add(1, std::memory_order_relaxed);
		// Yes! We got a slot on this page.
		if (static_cast<unsigned>(index) < EventsPerPage)
			return &oldTail->events[index];
	}

	// OK, we're definitely out of space. It's up to us to allocate a new page.
	Page *newTail = new Page;
	// Reserve the first slot.
	newTail->index.store(1, std::memory_order_relaxed);
	// A plain non-atomic move to oldTail->next is fine because there are no other writers here,
	// and nobody is supposed to read the logged contents until all logging is complete.
	oldTail->next = newTail;
	// m_tail must be written atomically because it is read concurrently from other threads.
	// We also use release/consume semantics so that its constructed contents are visible to other
	// threads. Again, very much like the double-checked locking pattern.
	m_tail.store(newTail, std::memory_order_release);

	if (m_numPages >= MaxNumPages)
	{
		Page *oldHead = m_head;
		m_head        = oldHead->next;
		delete oldHead;
	}
	else
	{
		m_numPages++;
	}

	// Return the reserved slot.
	return &newTail->events[0];
}

void
TraceLog::dumpStats()
{
	unsigned numEvents = 0;
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		numEvents = (m_numPages - 1) * EventsPerPage
		            + m_tail.load(std::memory_order_consume)->index.load(std::memory_order_relaxed);
	}
	printf("%u events logged\n", numEvents);
}

void
TraceLog::dumpEntireLog(const char *path, unsigned startPage)
{
	std::lock_guard<std::mutex> lock(m_mutex);
	FILE *f = path ? fopen(path, "w") : stderr;

	for (Page *page = m_head; page; page = page->next)
	{
		if (startPage > 0)
		{
			startPage--;
			continue;
		}

		unsigned limit =
		    std::min(EventsPerPage,
		             static_cast<unsigned>(page->index.load(std::memory_order_relaxed)));

		for (unsigned i = 0; i < limit; i++)
		{
			const Event &evt = page->events[i];

			fprintf(f, evt.fmt, evt.tid, evt.param1, evt.param2);
			fputc('\n', f);
		}
	}

	fclose(f);
}

} // namespace btree::utils