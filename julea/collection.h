#ifndef H_COLLECTION
#define H_COLLECTION

#include <list>

#include <boost/utility.hpp>

#include <mongo/db/jsobj.h>

namespace JULEA
{
	class Collection;
}

#include "item.h"
#include "ref_counted.h"
#include "store.h"

namespace JULEA
{
	class Collection : public RefCounted<Collection>
	{
		friend class Item;
		friend class Store;

		public:
			/*
			class Iterator
			{
				public:
					Iterator (Collection const& directory) : connection(FileSystem::Host()), directory(directory) { cursor =  connection->query("JULEA.DirectoryEntries", mongo::BSONObjBuilder().append("Collection", directory.m_id).obj()); };
					~Iterator () { connection.done(); };

					bool More () { return cursor->more(); };
					Item Next () { mongo::BSONObj f; string name; f = cursor->next(); name = f.getField("Name").String(); return Item(name); };
				private:
					mongo::ScopedDbConnection connection;
					Collection const& directory;
					std::auto_ptr<mongo::DBClientCursor> cursor;
			};
			*/

		public:
			Collection (string const&);

			string const& Name () const;

			Item* Get (string const&);
		private:
			Collection (Store*, string const&);
			Collection (Store*, mongo::BSONObj const&);
			~Collection ();

			mongo::BSONObj Serialize ();
			void Deserialize (mongo::BSONObj const&);

			mongo::OID const& ID () const;

			mongo::OID m_id;
			string m_name;
			std::list<Item*> newItems;

			Store* m_store;
	};
}

#endif
