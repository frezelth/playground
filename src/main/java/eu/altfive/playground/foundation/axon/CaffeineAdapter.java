package eu.altfive.playground.foundation.axon;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.axonframework.common.Registration;
import org.axonframework.common.caching.AbstractCacheAdapter;
import org.checkerframework.checker.nullness.qual.Nullable;

public class CaffeineAdapter extends AbstractCacheAdapter<RemovalListener<Object, Object>> {

  private final Cache<Object, Object> cache;

  public CaffeineAdapter(Cache<Object, Object> cache) {
    this.cache = cache;
  }

  @Override
  protected RemovalListener<Object, Object> createListenerAdapter(
      EntryListener cacheEntryListener) {
    return new CacheEventListenerAdapter(cacheEntryListener);
  }

  @Override
  protected Registration doRegisterListener(RemovalListener<Object, Object> listenerAdapter) {
    return null;
  }

  @Override
  public <K, V> V get(K key) {
    return (V)cache.getIfPresent(key);
  }

  @Override
  public void put(Object key, Object value) {
    cache.put(key, value);
  }

  @Override
  public boolean putIfAbsent(Object key, Object value) {
    return cache.asMap().putIfAbsent(key, value) == null;
  }

  @Override
  public boolean remove(Object key) {
    return cache.asMap().remove(key) != null;
  }

  @Override
  public boolean containsKey(Object key) {
    return cache.asMap().containsKey(key);
  }

  private static class CacheEventListenerAdapter implements RemovalListener<Object, Object> {

    private final EntryListener delegate;

    public CacheEventListenerAdapter(EntryListener delegate) {
      this.delegate = delegate;
    }

    @Override
    public void onRemoval(@Nullable Object key, @Nullable Object value, RemovalCause cause) {
      switch (cause){
        case EXPIRED -> delegate.onEntryExpired(key);
        case SIZE, REPLACED, EXPLICIT, COLLECTED -> delegate.onEntryRemoved(key);
      }
    }

  }
}
