package com.capstone.repository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import com.capstone.model.HouseInventory;

@Repository
public interface HousingInventoryRepository extends JpaRepository<HouseInventory, Long> {
    Optional<List<HouseInventory>> findByInventoryType(String inventoryType);
    Optional<List<HouseInventory>> findByState(String state);
    Optional<List<HouseInventory>> findByStateIn(Collection<String> states);

}
