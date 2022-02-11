address 0x1 {
module KeyValue {
    use Std::Event;
    use Std::Vector;
    use Std::Signer;

    struct Item has store, drop {
        key: u64,
        value: u64
    }

    struct Collection has key, store {
        items: vector<Item>
    }

    struct Get<phantom T: store + drop> has key, drop, store {
    }

    struct Set<phantom T: store + drop> has key, drop, store {
    }

    struct EventHandle<phantom T: store + drop> has key, store {
        event_handle: Event::EventHandle<T>
    }

    public fun set(account: &signer, key: u64, value: u64) acquires Collection, EventHandle {
        let owner = Signer::address_of(account);
        let items = &mut borrow_global_mut<Collection>(owner).items;
        let event_handle = &mut borrow_global_mut<EventHandle<Set<Item>>>(owner).event_handle;
        Vector::push_back<Item>(items, Item {key, value});
        Event::emit_event<Set<Item>>(event_handle, Set<Item> {});
    }

    public fun get(account: &signer, key: u64): u64 acquires Collection, EventHandle {
        let owner = Signer::address_of(account);
        let collection = borrow_global<Collection>(owner);
        let event_handle = &mut borrow_global_mut<EventHandle<Get<Item>>>(owner).event_handle;
        let i = 0;
        let len = Vector::length(&collection.items);
        while (i < len) {
            let item = Vector::borrow(&collection.items, i);
            if (item.key == key) {
                let value = item.value;
                Event::emit_event<Get<Item>>(event_handle, Get<Item> {});
                return value
            };
            i = i + 1;
        };
        0
    }

    public fun init(account: &signer) {
        move_to<Collection>(account, Collection {
            items: Vector::empty<Item>()
        });
        move_to<EventHandle<Set<Item>>>(account, EventHandle<Set<Item>> {
            event_handle: Event::new_event_handle<Set<Item>>(account)
        });
        move_to<EventHandle<Get<Item>>>(account, EventHandle<Get<Item>> {
            event_handle: Event::new_event_handle<Get<Item>>(account)
        });
    }
}
}
