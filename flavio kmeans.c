#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <math.h>
#include "mpi.h"
#define MASTER 0

typedef struct {
	double x;
	double y;
} Ponto;

void le_cabecalho(FILE *input, int* num_clusters, int* num_pontos){
	fscanf(input, "%d\n", num_clusters);
	printf("%d\n", *num_clusters);

	fscanf(input, "%d\n", num_pontos);
	printf("%d\n", *num_pontos);
}

void le_pontos(FILE* input, Ponto *pontos, int num_pontos){
	int i;
	for(i = 0; i < num_pontos; i++)
	{
		fscanf(input, "%lf,%lf", &pontos[i].x, &pontos[i].y);
	}
}

//gera centroides randomicamente
void inicializa(Ponto* centroides, int num_clusters){
	int i;
	srand(time(NULL));
	for(i = 0; i < num_clusters; i++){
		centroides[i].x = ((double)(rand() % 1000)) / 1000;
		centroides[i].y = ((double)(2 * rand() % 1000)) / 1000;
	}
}

//função para inicializar um dado vetor com -1
int reset_array(int *data, int num_pontos){
	int i;
	for(i = 0; i < num_pontos; i++){
		data[i] = -1;
	}		
}

double calcula_distancia(Ponto ponto1, Ponto ponto2){
	return (pow((ponto1.x - ponto2.x) * 100, 2) + pow((ponto1.y - ponto2.y) * 100, 2));	
}

int verifica_proximidade(Ponto ponto, Ponto* centroides, int num_centroides){
	int proximidade = 0;
	double distancia = 0;
	double minDistancia=calcula_distancia(ponto,centroides[0]);
	int i;
	
	for(i = 1; i < num_centroides; i++){	
		distancia = calcula_distancia(ponto, centroides[i]);
		if(minDistancia >= distancia){
			proximidade = i;
			minDistancia = distancia;
		}
	}
	return proximidade;
}

void recalcula_centroides(Ponto* pontos, int* data, Ponto* centroides, int num_clusters, int num_pontos){
	Ponto* novosCentroides = malloc(sizeof(Ponto) * num_clusters);
	int* n = malloc(sizeof(int) * num_clusters);
	int i;

	for(i = 0; i < num_clusters; i++){
		n[i] = 0;
		novosCentroides[i].x = 0;
		novosCentroides[i].y = 0;
	}	
	for(i = 0; i < num_pontos; i++){
		n[data[i]]++;
		novosCentroides[data[i]].x += pontos[i].x;
		novosCentroides[data[i]].y += pontos[i].y;
	}
	for(i = 0; i < num_clusters; i++){
		if(n[i] != 0.0){
			novosCentroides[i].x /= n[i];
			novosCentroides[i].y /= n[i];
		}
	}
	for(i = 0; i < num_clusters; i++){
		centroides[i].x = novosCentroides[i].x;
		centroides[i].y = novosCentroides[i].y;
	}	
}

//-1 se não convergir, 0 caso contrário
int checa_convergencia(int *clusters_antigos, int *clusters_novos, int num_pontos){
	int i;
	for(i = 0; i < num_pontos; i++){
		if(clusters_antigos[i] != clusters_novos[i]){
			return -1;
		}
	}
	return 0;
}

int main(int argc, char* argv[]){
	int rank;
	int tamanho;
	int num_clusters;
	int num_pontos;
	int i;
	int tamanho_job;
	int job_terminado = 0;
	
	Ponto* centroides;
	Ponto* pontos;
	Ponto* pontos_recebidos;
	int* slave_clusters;
	int* clusters_antigos;
	int* clusters_novos;
    	
	MPI_Init(&argc, &argv);
	
	MPI_Status status;

	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &tamanho);
	
	MPI_Datatype MPI_POINT;
	MPI_Datatype type = MPI_DOUBLE;
	int blocklen = 2;
	MPI_Aint disp = 0;
	MPI_Type_create_struct(1, &blocklen, &disp, &type, &MPI_POINT);
	MPI_Type_commit(&MPI_POINT);

      
   	if(rank == MASTER)
  	{
		//lendo arquivo de entrada
		FILE *input;
		input=fopen(argv[1], "r");
		le_cabecalho(input, &num_clusters, &num_pontos);
		pontos = (Ponto*)malloc(sizeof(Ponto) * num_pontos);
		le_pontos(input, pontos, num_pontos);
		fclose(input);

		clusters_antigos = (int*)malloc(sizeof(int) * num_pontos);
		clusters_novos = (int*)malloc(sizeof(int) * num_pontos);
		tamanho_job = num_pontos / (tamanho-1);
		centroides = malloc(sizeof(Ponto) * num_clusters);
				
		inicializa(centroides, num_clusters);
		reset_array(clusters_antigos, num_pontos);
		reset_array(clusters_novos, num_pontos);
		
		for(i = 1; i < tamanho; i++){
			printf("Enviando para [%d]\n", i);
			MPI_Send(&tamanho_job, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
			MPI_Send(&num_clusters, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
			MPI_Send(centroides, num_clusters, MPI_POINT, i, 0, MPI_COMM_WORLD);
			MPI_Send(pontos+(i-1) * tamanho_job, tamanho_job, MPI_POINT, i, 0, MPI_COMM_WORLD);
		}
    		printf("Enviado!\n");

		MPI_Barrier(MPI_COMM_WORLD);
	
		while(1){	
			MPI_Barrier(MPI_COMM_WORLD);
			printf("Master recebendo\n");
			for(i = 1; i < tamanho; i++){
				MPI_Recv(clusters_novos + (tamanho_job * (i - 1)), tamanho_job, MPI_INT, i, 0, MPI_COMM_WORLD, &status);
			}
			
			printf("Master recebeu\n");
			
			recalcula_centroides(pontos, clusters_novos, centroides, num_clusters, num_pontos);
			printf("Novos centroides prontos!\n");
			if(checa_convergencia(clusters_novos, clusters_antigos, num_pontos) == 0){
				printf("Convergiu!\n");
				job_terminado = 1;
			}
			else{
				printf("Não convergiu!\n");
				for(i = 0; i < num_pontos; i++){
					clusters_antigos[i] = clusters_novos[i];
				}
			}
			
			for( i = 1; i < tamanho; i++){
				MPI_Send(&job_terminado, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
			}

			MPI_Barrier(MPI_COMM_WORLD);
			if(job_terminado == 1){
				break;
			}
				
			for(i = 1; i < tamanho; i++){
				MPI_Send(centroides, num_clusters, MPI_POINT, i, 0, MPI_COMM_WORLD);
			}

			MPI_Barrier(MPI_COMM_WORLD);
		}
		
		//Escrevendo arquivo de saída		
		FILE* output = fopen(argv[2],"w");
		fprintf(output, "%d\n", num_clusters);
		fprintf(output, "%d\n", num_pontos);
		for(i = 0; i < num_clusters; i++){
			fprintf(output, "%lf, %lf\n", centroides[i].x, centroides[i].y);
		}
		for(i = 0; i < num_pontos; i++){
			fprintf(output, "%lf, %lf, %d\n", pontos[i].x, pontos[i].y, clusters_novos[i] + 1);
		}
		fclose(output);
	}

	else{
		printf("Recebendo\n");
		MPI_Recv(&tamanho_job, 1, MPI_INT, MASTER, 0, MPI_COMM_WORLD, &status);
		MPI_Recv(&num_clusters, 1, MPI_INT, MASTER, 0, MPI_COMM_WORLD, &status);
		centroides=malloc(sizeof(Ponto) * num_clusters);
		MPI_Recv(centroides, num_clusters, MPI_POINT, MASTER, 0, MPI_COMM_WORLD, &status);
		printf("tamanho_job = %d\n", tamanho_job);
		pontos_recebidos=(Ponto*)malloc(sizeof(Ponto) * tamanho_job);
		slave_clusters=(int*)malloc(sizeof(int) * tamanho_job);
		MPI_Recv(pontos_recebidos, tamanho_job, MPI_POINT, MASTER, 0, MPI_COMM_WORLD, &status);
		printf("Recebido [%d]\n", rank);

		MPI_Barrier(MPI_COMM_WORLD);
		
		while(1){
			printf("Calculo dos novos clusters [%d]\n", rank);
			for(i = 0; i < tamanho_job; i++){
				slave_clusters[i] = verifica_proximidade(pontos_recebidos[i], centroides, num_clusters);
			}
			
			printf("Enviando para o master [%d]\n", rank);
			MPI_Send(slave_clusters, tamanho_job, MPI_INT,MASTER, 0, MPI_COMM_WORLD);
			MPI_Barrier(MPI_COMM_WORLD);
			MPI_Barrier(MPI_COMM_WORLD);
			MPI_Recv(&job_terminado, 1, MPI_INT, MASTER, 0, MPI_COMM_WORLD, &status);
					
			if(job_terminado==1){
				break;
			}
			
			MPI_Recv(centroides, num_clusters, MPI_POINT, MASTER, 0, MPI_COMM_WORLD, &status);

			MPI_Barrier(MPI_COMM_WORLD);
		}
	}

	MPI_Finalize();
	return 0;
}