package com.google.appengine.tools.pipeline.impl;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.pipeline.PromisedValue;
import com.google.appengine.tools.pipeline.impl.model.Slot;

public class PromisedValueImpl<E> extends FutureValueImpl<E> implements PromisedValue<E> {
	
	public PromisedValueImpl(Key rootJobGuid){
		super(new Slot(rootJobGuid));
	}
	
	public String getHandle(){
		return KeyFactory.keyToString(slot.getKey());
	}

}
