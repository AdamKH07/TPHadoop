!pip install --quiet mrjob==0.6
from mrjob.job import MRJob
from mrjob.step import MRStep

class CountTagsByUser(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_tags_by_user,
                   reducer=self.reducer_count_tags)
        ]

    def mapper_get_tags_by_user(self, _, line):
        try:
            # Séparer les colonnes du fichier tags.csv par des virgules
            user_id, movie_id, tag, timestamp = line.split(',')
            yield user_id, 1  # Utiliser user_id comme clé, émettre le tag avec une valeur de 1
        except ValueError:
            # Ignorer les lignes qui ne peuvent pas être correctement traitées
            pass
        except Exception as e:
            # Journaliser les exceptions inattendues
            self.log.warning("Erreur inattendue: %s" % e)

    def reducer_count_tags(self, user_id, counts):
        # Somme de tous les 1 pour chaque user_id, donnant le nombre total de tags ajoutés par l'utilisateur
        yield user_id, sum(counts)

if __name__ == '__main__':
    CountTagsByUser.run()
