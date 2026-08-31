package org.example.producer.controller;

import org.example.producer.service.KafkaProducerService;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class KafkaControllerTests {

    private final KafkaProducerService producerService = mock(KafkaProducerService.class);
    private final MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new KafkaController(producerService)).build();

    @Test
    void acceptsEmployeeCreationRequest() throws Exception {
        mockMvc.perform(post("/employees")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id\":101,\"name\":\"Deepak\",\"department\":\"Engineering\"}"))
                .andExpect(status().isAccepted());

        verify(producerService).sendEmployee(argThat(employee ->
                employee.getId().equals(101L)
                        && employee.getName().equals("Deepak")
                        && employee.getDepartment().equals("Engineering")
        ));
    }

    @Test
    void acceptsEmployeeDeletionRequest() throws Exception {
        mockMvc.perform(delete("/employees/101"))
                .andExpect(status().isAccepted());

        verify(producerService).deleteEmployee(101L);
    }
}
