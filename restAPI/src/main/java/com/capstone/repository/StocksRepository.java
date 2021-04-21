package main.java.com.capstone.repository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.capstone.model.StockLine;

@Repository
public interface StocksRepository extends JpaRepository<StockLine, Long> {
    Optional<List<StockLine>> findBySymbol(String symbol);
    Optional<List<StockLine>> findBySymbolIn(Collection<String> symbols);
}
