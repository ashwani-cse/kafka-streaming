package com.orders.domain;

import java.time.LocalDateTime;

public record OrderCountPerStoreByWindowsDTO(String locationId,
                                             Long orderCount,
                                             OrderType orderType,
                                             LocalDateTime startWindow,
                                             LocalDateTime endWindow) {
}
