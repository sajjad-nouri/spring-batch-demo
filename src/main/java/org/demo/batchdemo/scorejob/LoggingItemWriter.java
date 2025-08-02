package org.demo.batchdemo.scorejob;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;

@Getter
@Setter
public class LoggingItemWriter<T> implements ItemWriter<T>, ItemStream {
    private final ItemWriter<T> delegate;
    private final ItemStream streamDelegate;

    public LoggingItemWriter(ItemWriter<T> delegate) {
        this.delegate = delegate;
        this.streamDelegate = (ItemStream) delegate; // FlatFileItemWriter implements ItemStream
    }

    @Override
    public void write(Chunk<? extends T> chunk) throws Exception {
        System.out.println("LoggingItemWriter write: " + chunk.getItems());
        delegate.write(chunk);
    }

    @Override
    public void open(org.springframework.batch.item.ExecutionContext executionContext) throws ItemStreamException {
        streamDelegate.open(executionContext);
    }

    @Override
    public void update(org.springframework.batch.item.ExecutionContext executionContext) throws ItemStreamException {
        streamDelegate.update(executionContext);
    }

    @Override
    public void close() throws ItemStreamException {
        streamDelegate.close();
    }
}
